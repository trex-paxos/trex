package com.github.trex_paxos

import java.util.concurrent.TimeoutException

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, ActorSelection}
import akka.serialization.{SerializationExtension, Serializer}
import akka.util.Timeout
import com.github.trex_paxos.BaseDriver.SelectionUrlFactory
import com.github.trex_paxos.internals.ClientRequestCommandValue
import com.github.trex_paxos.library.{LostLeadershipException, _}

import scala.collection.SortedMap
import scala.compat.Platform
import scala.concurrent.duration._

object BaseDriver {
  type SelectionUrlFactory = (ActorContext, Cluster) => Map[Int, String]

  def defaultSelectionUrlFactory(context: ActorContext, cluster: Cluster): Map[Int, String] = {
    val nodeAndUrl = cluster.nodes.map { node =>
      (node.nodeUniqueId, s"akka.tcp://${cluster.name}@${node.host}:${node.clientPort}/user/PaxosActor")
    }
    (cluster.nodes.indices zip nodeAndUrl.map( {case (_, s) => s} )).toMap
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
  * @param maxAttempts The maximum number of timeout retry attempts on any message.
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
  protected def resolveActorSelectorForIndex(memberIndex: Int): ActorSelection

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
    requestId
  }

  case class Request(timeoutTime: Long, client: ActorRef, command: CommandValue, attempt: Int)

  /**
    * lookup a request by its identifier
    */
  private[this] var requestById: SortedMap[Long, Request] = SortedMap.empty

  /**
    * look up requests by the timeout then by identifier
    * due to clock resolution we can have multiple requests timeout in the same millisecond
    */
  private[this] var requestByTimeoutById: SortedMap[Long, Map[Long, Request]] = SortedMap.empty

  def hold(request: Request): Unit = {
    requestById = requestById + (request.command.msgId -> request)
    val peers = requestByTimeoutById.getOrElse(request.timeoutTime, Map.empty)
    requestByTimeoutById = requestByTimeoutById + (request.timeoutTime -> (peers + (request.command.msgId -> request)))
  }

  def drop(request: Request): Unit = {
    requestById = requestById - request.command.msgId
    val peers = requestByTimeoutById.getOrElse(request.timeoutTime, Map.empty)
    val updated = peers - request.command.msgId
    if (updated.isEmpty) {
      requestByTimeoutById = requestByTimeoutById - request.timeoutTime
    } else {
      requestByTimeoutById = requestByTimeoutById + (request.timeoutTime -> updated)
    }
  }

  def swap(out: Request, in: Request): Unit = {
    require(in.command.msgId == out.command.msgId)
    drop(out)
    hold(in)
  }

  // TODO is this cache necessary surely akka caches itself?
  private[this] var serializers: Map[Class[_], Serializer] = Map.empty

  def getSerializer(clazz: Class[_]) = {
    val serializer = serializers.get(clazz)
    serializer match {
      case Some(s) => s
      case None =>
        val s = SerializationExtension(context.system).serializerFor(clazz)
        serializers = serializers + (clazz -> s)
        s
    }
  }

  /**
    * override for tests
    */
  def now() = Platform.currentTime

  override def receive: Receive = {
    /**
      * If we got back a response from the cluster send it back up the stack to the client actor.
      */
    case ServerResponse(id, responseOption) =>
      if (log.isDebugEnabled) log.debug("{} found is {} is in map {}", id, requestById.contains(id), requestById)
      requestById.get(id) foreach {
        case request@Request(_, client, _, _) =>
          responseOption foreach { response =>
            client ! response
            log.debug("response {} for {} is {}", id, client, response)
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

    case msg: Any =>
      outboundClientWork(leaderCounter, msg.asInstanceOf[AnyRef])
  }

  def resend(out: Request): Unit = {
    val in = out.copy(attempt = out.attempt + 1, timeoutTime = now + timeoutMillis)
    swap(out, in)
    val target = resolveActorSelectorForIndex((leaderCounter % clusterSize).toInt)
    target ! in.command
    log.debug("resent request from {} to {} is {}", sender(), target, in.command)
  }

  def outboundClientWork(counter: Long, work: AnyRef): Unit = {
    val bytes = getSerializer(work.getClass).toBinary(work)
    val commandValue = ClientRequestCommandValue(incrementAndGetRequestId(), bytes)
    val request = Request(Platform.currentTime + timeoutMillis, sender(), commandValue, 1)
    val target = resolveActorSelectorForIndex((counter % clusterSize).toInt)
    target ! commandValue
    hold(request)
    log.debug("sent normal request from {} to {} is {} containing {}", sender(), target, commandValue, work)
  }
}

/**
  * A concrete driver which uses akka.tcp to send messages. Note akka documentation says that akka.tcp is not firewall friendly.
  *
  * @param timeout The client timeout. It is recommended that this is significantly longer than the cluster failover timeout.
  * @param cluster The static cluster members.
  */
class StaticClusterDriver(timeout: Timeout,
                          cluster: Cluster,
                          maxAttempts: Int,
                          factory: SelectionUrlFactory = BaseDriver.defaultSelectionUrlFactory _ )
  extends BaseDriver(timeout, maxAttempts) with ActorLogging {

  import scala.concurrent.ExecutionContext.Implicits.global

  // TODO inject these timings from config
  context.system.scheduler.schedule(Duration(5, MILLISECONDS), Duration(1000, MILLISECONDS), self, CheckTimeout)

  val selectionUrls = factory(context, cluster)

  log.info("selections are: {}", selectionUrls)

  // cluster membership is static
  def resolveActorSelectorForIndex(node: Int): ActorSelection = {
    val url = selectionUrls(node)
    val result = context.actorSelection(url)
    result
  }

  /**
    * Returns the current cluster size.
    */
  override protected def clusterSize: Int = cluster.nodes.size
}

