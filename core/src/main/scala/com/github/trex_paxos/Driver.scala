package com.github.trex_paxos

import java.util.concurrent.TimeoutException

import _root_.akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, ActorSelection}
import _root_.akka.serialization.{SerializationExtension, Serializer}
import _root_.akka.util.Timeout
import _root_.com.github.trex_paxos.internals._
import _root_.com.github.trex_paxos.library.{LostLeadershipException, _}

import scala.collection.SortedMap
import scala.compat.Platform
import scala.concurrent.duration._

object BaseDriver {
  type SelectionUrlFactory = (ActorContext, Cluster) => Map[Int, String]

  def defaultSelectionUrlFactory(context: ActorContext, cluster: Cluster): Map[Int, String] = {
    val nodeAndUrl = cluster.nodes.map { node =>
      (node.nodeUniqueId, s"akka.tcp://${cluster.name}@${node.host}:${node.clientPort}/user/PaxosActor")
    }
    (cluster.nodes.indices zip nodeAndUrl.map({ case (_, s) => s })).toMap
  }
}

/**
  * Driver baseclass logic forwards messages to a trex cluster switching nodes until it finds the leader.
  * The driver coverts the application command value into a byte array ready to journal on the server.
  *
  * FIXME we retry on timeout yet we don't know if the command happened on a timeout and whether it is safe to retry
  * we should have a map of what client messages types are known safe to retry (e.g. read-only) and only retry those.
  *
  * @param requestTimeout Timeout on individual requests.
  * @param maxAttempts    The maximum number of timeout retry attempts on any message.
  */
abstract class BaseDriver(requestTimeout: Timeout, maxAttempts: Int) extends Actor with ActorLogging {

  val timeoutMillis = requestTimeout.duration.toMillis

  /**
    * Returns an ActorSelection mapped to the passed cluster membership index.
    * This is abstract so that there can be a subclass which knows about cluster membership changes.
    * Counter can be incremented to round-robin to find the new stable leader.
    *
    * @param memberIndex The index of the node in the cluster to resolve.
    */
  protected def resolveActorSelectorForIndex(memberIndex: Int): Option[ActorSelection]

  /**
    * Returns the current cluster size.
    */
  protected def clusterSize: Int

  /**
    * This counter modulo the cluster size is used to pick a member of the cluster to send leader messages.
    * If we get back a response that the node isn't the leader the driver will increment the counter to try the next node.
    */
  protected var leaderCounter: Long = 0

  /**
    * Incremented for each request sent.
    */
  private[this] var requestId: Long = 0

  def incrementAndGetRequestId() = {
    requestId = requestId + 1
    requestId.toString
  }

  case class Request(timeoutTime: Long, client: ActorRef, command: CommandValue, attempt: Int)

  /**
    * lookup a request by its identifier
    */
  private[this] var requestById: SortedMap[String, Request] = SortedMap.empty

  /**
    * look up requests by the timeout then by identifier
    * due to clock resolution we can have multiple requests timeout in the same millisecond
    */
  private[this] var requestByTimeoutById: SortedMap[Long, Map[String, Request]] = SortedMap.empty

  def hold(request: Request): Unit = {
    requestById = requestById + (request.command.msgUuid -> request)
    val peers = requestByTimeoutById.getOrElse(request.timeoutTime, Map.empty)
    requestByTimeoutById = requestByTimeoutById + (request.timeoutTime -> (peers + (request.command.msgUuid -> request)))
  }

  def drop(request: Request): Unit = {
    requestById = requestById - request.command.msgUuid
    val peers = requestByTimeoutById.getOrElse(request.timeoutTime, Map.empty)
    val updated = peers - request.command.msgUuid
    if (updated.isEmpty) {
      requestByTimeoutById = requestByTimeoutById - request.timeoutTime
    } else {
      requestByTimeoutById = requestByTimeoutById + (request.timeoutTime -> updated)
    }
  }

  def swap(out: Request, in: Request): Unit = {
    require(in.command.msgUuid == out.command.msgUuid)
    drop(out)
    hold(in)
  }

  /**
    * Akka lets you configure differ serializers per class and defaults to the built in java serializer.
    * Here we abuse that slightly by letting you configure a serializer for ClientCommandValue and
    * that serializer will be used to configure all application request/response type into bytes. The
    * actual ClientCommandValue wrapper will be pickled with the built in pickler. Note that if you host
    * can send back exceptions and you configure a custom serializer you need to ensure that it can roundtrip
    * your application exceptions.
    */
  val serializerClient = SerializationExtension(context.system).serializerFor(new ClientCommandValue("", Array.empty[Byte]).getClass)

  /**
    * override for tests
    */
  def now() = Platform.currentTime

  override def receive: Receive = {
    /**
      * If we got back a response from the cluster send it back up the stack to the client actor.
      */
    case ServerResponse(slot, cid, responseOption) =>
      if (log.isDebugEnabled) log.debug("slot {} with {} found is {} is in map {}", slot, cid, requestById.contains(cid), requestById)
      requestById.get(cid) foreach {
        case request@Request(_, client, _, _) =>
          responseOption foreach { response =>
            client ! serializerClient.fromBinary(response)
            log.debug("response {} for {} is {}", cid, client, response)
          }
          drop(request)
      }

    /**
      * Check for timed-out work. Any work that has had too many retry attempts we send an exception back up the stack
      * to the client. Any work that has not yet had too many retry attempts we forward to the next node in the cluster.
      */
    case CheckTimeout =>
      val ts = now()
      val overdue = requestByTimeoutById.takeWhile { case (timeout, _) => ts > timeout }
      if (overdue.nonEmpty) {
        leaderCounter = leaderCounter + 1
        log.debug("incremented counter to {}", leaderCounter)
        val overdueRequests: Iterable[Request] = overdue flatMap {
          case (_, r) =>
            r map {
              case (_, request) =>
                request
            }
        }
        val (overAttempts, underAttempts) = overdueRequests.seq.partition {
          _.attempt >= maxAttempts
        }
        overAttempts foreach { overAttempt =>
          drop(overAttempt)
          overAttempt.client ! new TimeoutException(s"Exceeded maxAttempts $maxAttempts")
        }
        underAttempts foreach { out =>
          resend(out)
        }
      }

    /**
      * The node we sent the request to is not the leader.
      */
    case m@NotLeader(node, msgId) =>
      // FIXME put the leader node into request and ignore message if its not from the node we last sent to
      val requestOpt = requestById.get(msgId)
      log.info("{} for request {} in requests {}", m, requestOpt, requestById)
      requestById.get(msgId) foreach {
        case out: Request =>
          // FIXME check "leaderCounter % clusterSize == node" before incrementing the leader counter!
          leaderCounter = leaderCounter + 1
          resend(out)
        case _ =>
          log.error("unreachable code but the code analyzer nags me about incomplete matches")
      }

    /**
      * The node we sent the message to was a leader but has lost the leadership. Trex doesn't know what to do next
      * as per the documentation on the exception [[LostLeadershipException]].
      */
    case nlle: LostLeadershipException =>
      // TODO
      log.error("node {} responded to message {} with an NoLongerLeaderException so we don't know whether the command succeeded or failed: {}", nlle.nodeId, nlle.msgId, nlle.getMessage)
      requestById.get(nlle.msgId) foreach {
        case request@Request(_, client, _, _) =>
          client ! nlle
          drop(request)
      }

    case work: AnyRef =>
      val bytes = serializerClient.toBinary(work)
      val commandValue = ClientCommandValue(incrementAndGetRequestId(), bytes)
      val request = Request(Platform.currentTime + timeoutMillis, sender(), commandValue, 1)
      transmit(leaderCounter, request)
  }

  /**
    * Send a message to the cluster
    * @param counter A counter which is used to pick a node to transmit to.
    * @param request A request to send.
    */
  def transmit(counter: Long, request: Request) = {
    val target = resolveActorSelectorForIndex((counter % clusterSize).toInt)
    target match {
      case Some(t) =>
        t ! request.command
        hold(request)
        log.debug("sent request from {} to {} is {}", sender(), target, request.command)
      case None =>
        log.warning("unable to send work as target is None")
    }
  }

  def resend(out: Request): Unit = {
    val in = out.copy(attempt = out.attempt + 1, timeoutTime = now + timeoutMillis)
    swap(out, in)
    val target = resolveActorSelectorForIndex((leaderCounter % clusterSize).toInt)
    target match {
      case Some(t) =>
        t ! in.command;
        log.debug("resent request from {} to {} is {}", sender(), t, in.command)
      case None =>
        log.warning("unable to send request as target is None")
    }
  }

}

object DynamicClusterDriver {
  case class Initialize(membership: Membership)
  def apply(cluster: Cluster) = {
    DynamicClusterDriver.Initialize(Membership(cluster.name, cluster.nodes.map { node =>
      Member(node.nodeUniqueId, s"${node.host}:${node.nodePort}", s"${node.host}:${node.clientPort}", Accepting)
    }))
  }
}

/**
  * A concrete driver which uses akka.tcp to send messages. FIXME Note akka documentation says that akka.tcp is not firewall friendly.
  *
  * @param timeout The client timeout. It is recommended that this is significantly longer than the cluster failover timeout.
  * @param maxAttempts The number of retries before the driver fails. Should be set greater or equal to the max cluster size.
  */
class DynamicClusterDriver(timeout: Timeout, maxAttempts: Int) extends BaseDriver(timeout, maxAttempts) with ActorLogging {

  import scala.concurrent.ExecutionContext.Implicits.global

  // TODO inject these timings from config
  context.system.scheduler.schedule(Duration(5, MILLISECONDS), Duration(1000, MILLISECONDS), self, CheckTimeout)

  var membership: Option[CommittedMembership] = None // TODO do we warn if this is None and the actor is uninitialised

  case object CheckMembership

  // TODO inject these timings from config
  context.system.scheduler.schedule(Duration(505, MILLISECONDS), Duration(1000, MILLISECONDS), self, CheckMembership)

  def localReceive: Receive = {
    case i: DynamicClusterDriver.Initialize =>
      membership = Option(CommittedMembership(Long.MinValue, i.membership))
      log.info("membership initialized to {}", membership)
    case CheckMembership =>
      // FIXME
      // pick a random node and tell it about our latest knowledge of committed membership. if we are out of date it will reply
      /*
      membership foreach {
        case c: CommittedMembership =>
          resolveActorSelectorForIndex(((Math.random() * 1024) % clusterSize).toInt) foreach {
            _ ! c
          }
      }*/
    case work: Membership =>
      // FIXME
//      log.info("received membership change request {}", work)
//      val commandValue = MembershipCommandValue(incrementAndGetRequestId(), work)
//      val request = Request(Platform.currentTime + timeoutMillis, sender(), commandValue, 1)
//      transmit(leaderCounter, request)
    case m: CommittedMembership =>
      // update our knowledge of the commitment membership of the cluster
      membership = Option(m)
      log.info(s"membership changed to ${m}")
  }

  override def receive: Receive = localReceive orElse super.receive

  /**
    * Returns the current cluster size.
    */
  override protected def clusterSize: Int = membership match {
    case None => 1 // TODO zero here gives divide by zero. 1 here is handled else where in the stack.
    case Some(m) => m.membership.members.size
  }

  /**
    * Returns an ActorSelection mapped to the passed cluster membership index.
    * This is abstract so that there can be a subclass which knows about cluster membership changes.
    * Counter can be incremented to round-robin to find the new stable leader.
    *
    * @param memberIndex The index of the node in the cluster to resolve. Usually computed as counter%size
    */
  override protected def resolveActorSelectorForIndex(memberIndex: Int): Option[ActorSelection] = {

    membership map { m =>
      val urls = m.membership.members.map { node =>
         s"akka.tcp://${m.membership.name}@${node.clientLocation}/user/PaxosActor"
       }
      val indexToUrl = (m.membership.members.indices zip urls).toMap
      context.actorSelection(indexToUrl(memberIndex))
    }
  }
}