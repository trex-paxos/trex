package com.github.trex_paxos

import java.net.InetSocketAddress
import java.util.concurrent.TimeoutException

import akka.actor.TypedActor.MethodCall
import akka.actor._
import akka.event.Logging
import akka.io.Udp.CommandFailed
import akka.io.{IO, Udp}
import akka.serialization._
import akka.util.Timeout
import com.github.trex_paxos.internals.{PaxosActor, PaxosActorWithTimeout, Pickle}
import com.github.trex_paxos.library._
import com.typesafe.config.Config

import scala.collection.SortedMap
import scala.compat.Platform
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

// TODO do we really need a custom extension can we not use SerializationExtension directly
class Trex(system: ExtendedActorSystem) extends Extension {
  val serialization = SerializationExtension(system)
  val serializer = serialization.serializerFor(classOf[MethodCall])

  def serialize(methodCall: MethodCall): Array[Byte] = {
    serializer.toBinary(methodCall)
  }

  def deserialize(bytes: Array[Byte]): MethodCall = {
    serializer.fromBinary(bytes, manifest = None).asInstanceOf[MethodCall]
  }

}

object TrexExtension extends ExtensionId[Trex] with ExtensionIdProvider {
  override def createExtension(system: ExtendedActorSystem): Trex = new Trex(system)

  override def lookup(): ExtensionId[_ <: Extension] = TrexExtension
}

class UdpSender(remote: InetSocketAddress) extends Actor with ActorLogging {

  import context.system

  IO(Udp) ! Udp.SimpleSender

  def receive = {
    case "status" =>
      sender ! false // not yet ready
    case Udp.SimpleSenderReady =>
      log.info("ready to send")
      context.become(ready(sender()))
    case unknown => // we drop messages if we are not connected as paxos makes this safe
      log.debug("Sender to {} dropping {}", remote, unknown)
  }

  def ready(connection: ActorRef): Receive = {
    case "status" =>
      sender ! true // ready
    case msg: AnyRef =>
      log.debug("sending to {} msg {}", remote, msg)
      val packed = Pickle.pack(msg)
      if (packed.size > 65535) log.warning("message size > 65,535 may not fit in UDP package")
      connection ! Udp.Send(Pickle.pack(msg), remote) // makes defensive copy
    case unknown =>
      log.warning("{} connected got unknown message {}", this.getClass.getCanonicalName, unknown)
  }
}

class UdpListener(socket: InetSocketAddress, nextActor: ActorRef) extends Actor with ActorLogging {

  import context.system

  log.info("Binding UdpListener to {}", socket)
  IO(Udp) ! Udp.Bind(self, socket)

  def receive = {
    case "status" =>
      sender ! false // not yet ready
    case Udp.Bound(local) =>
      log.info("Bound UdpListener to {}", socket)
      context.become(ready(sender()))
    case c: CommandFailed =>
      log.error(s"failed to bind to $socket due to {}", c)
      context.stop(self)
    case unknown =>
      log.error("bound listener for {} unknown message {}", socket, unknown.getClass)
  }

  def ready(s: ActorRef): Receive = {
    case "status" =>
      log.info("successfully bound to {}", socket)
      sender ! true // ready
    case Udp.Received(data, remote) =>
      val msg = Pickle.unpack(data)
      log.debug("received from {} {}", remote, msg)
      nextActor ! msg
    case Udp.Unbind => s ! Udp.Unbind
    case Udp.Unbound => context.stop(self)
    case unknown =>
      log.error("bound listener for {} unknown message {}", socket, unknown.getClass)
  }
}

class Broadcast(peers: Seq[ActorRef]) extends Actor with ActorLogging {
  override def receive: Actor.Receive = {
    case msg => peers foreach {
      log.debug("Broadcast sending {} to peers {}", msg, peers)
      _ ! msg
    }
  }
}

/**
 * Driver baseclass logic forwards messages to a trex cluster switching nodes until it finds the leader.
 * The driver coverts the application command value into a byte array ready to journal on the server.
 * @param requestTimeout Timeout on individual requests.
 */
abstract class BaseDriver(requestTimeout: Timeout, maxAttempts: Int) extends Actor with ActorLogging {

  val timeoutMillis = requestTimeout.duration.toMillis

  /**
   * Returns an ActorSelection mapped to the passed counter.
   * This is abstract so that there can be a subclass which knows about cluster membership changes.
   * Counter can be incremented to round-robin to find the new stable leader.
   */
  protected def resolve(member: Int): ActorSelection

  /**
   * Incremented to try the next node in the cluster.
   */
  protected var counter = 0

  /**
   * Incremented for each request sent. 
   */
  private[this] var requestId: Long = 0

  def incrementAndGetRequestId() = {
    requestId = requestId + 1
    requestId
  }

  case class Request(timeoutTime: Long, client: ActorRef, command: ClientRequestCommandValue, attempt: Int)

  private[this] var requestById: SortedMap[Long, Request] = SortedMap.empty
  private[this] var requestByTimeoutById: SortedMap[Long, Map[Long, Request]] = SortedMap.empty

  def add(request: Request): Unit = {
    requestById = requestById + (request.command.msgId -> request)
    val peers = requestByTimeoutById.getOrElse(request.timeoutTime, Map.empty)
    requestByTimeoutById = requestByTimeoutById + (request.timeoutTime -> (peers + (request.command.msgId -> request)))
  }

  def remove(request: Request): Unit = {
    requestById = requestById - request.command.msgId
    val peers = requestByTimeoutById.getOrElse(request.timeoutTime, Map.empty)
    val updated = peers - request.command.msgId
    if (updated.isEmpty) {
      requestByTimeoutById = requestByTimeoutById - request.timeoutTime
    } else {
      requestByTimeoutById = requestByTimeoutById + (request.timeoutTime -> updated)
    }
  }

  def replace(out: Request, in: Request): Unit = {
    require(in.command.msgId == out.command.msgId)
    remove(out)
    add(in)
  }

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

  def now() = Platform.currentTime // override for tests

  override def receive: Receive = {
    case CheckTimeout =>
      val ts = now()
      val overdue = requestByTimeoutById.takeWhile { case (timeout, _) => ts > timeout }
      if (overdue.nonEmpty) {
        counter = counter + 1
        log.debug("incremented counter to {}", counter)
      }
      val requests: Iterable[Request] = overdue flatMap {
        case (_, r) =>
          r map {
            case (_, request) =>
              request
          }
      }
      val (overAttempts, underAttempts) = requests.seq.partition {
        _.attempt > maxAttempts - 1
      }
      overAttempts foreach { overAttempt =>
        remove(overAttempt)
        overAttempt.client ! new TimeoutException(s"Exceeded maxAttempts $maxAttempts")
      }
      underAttempts foreach { out =>
        val in = out.copy(attempt = out.attempt + 1, timeoutTime = now + timeoutMillis)
        replace(out, in)
        val target = resolve(counter)
        target ! in.command
        log.debug("resent request from {} to {} is {}", sender(), target, in.command)
      }

    case NotLeader(node, msgId) =>
      log.info("Server node {} responded to msg {} that it is NotLeader", node, msgId)
      requestById.get(msgId) foreach {
        case out: Request =>
          // TODO do we want to validate that we were waiting for a message from that node?
          counter = counter + 1
          val in = out.copy(attempt = out.attempt + 1, timeoutTime = now + timeoutMillis)
          replace(out, in)
          val target = resolve(counter)
          target ! in.command
          log.debug("resent request from {} to {} is {}", sender(), target, in.command)
      }

    case nlle: NoLongerLeaderException =>
      log.error("node {} responded to message {} with an exception: {}", nlle.nodeId, nlle.msgId, nlle.getMessage)
      requestById.get(nlle.msgId) foreach {
        case request@Request(_, client, _, _) =>
          client ! nlle
          remove(request)
      }

    case ServerResponse(id, responseOption) =>
      if (log.isDebugEnabled) log.debug("{} found is {} is in map {}", id, requestById.contains(id), requestById)
      requestById.get(id) foreach {
        case request@Request(_, client, _, _) =>
          responseOption foreach { response =>
            client ! response
            log.debug("response {} for {} is {}", id, client, response)
          }
          remove(request)
      }
    case msg: Any =>
      val bytes = getSerializer(msg.getClass).toBinary(msg.asInstanceOf[AnyRef])
      val commandValue = ClientRequestCommandValue(incrementAndGetRequestId(), bytes)
      val request = Request(Platform.currentTime + timeoutMillis, sender(), commandValue, 1)
      val target = resolve(counter)
      target ! commandValue
      add(request)
      log.debug("sent request from {} to {} is {} containing {}", sender(), target, commandValue, msg)
  }
}

/**
 * A concrete driver which uses akka.tcp to send messages. Note akka documentation says that akka.tcp is not firewall friendly.
 * @param timeout The client timeout. It is recommended that this is significantly longer than the cluster failover timeout.
 * @param cluster The static cluster members.
 */
class StaticClusterDriver(timeout: Timeout, cluster: Cluster, maxAttempts: Int) extends BaseDriver(timeout, maxAttempts) with ActorLogging {

  import scala.concurrent.ExecutionContext.Implicits.global

  // TODO inject these timings from config
  context.system.scheduler.schedule(Duration(5, MILLISECONDS), Duration(1000, MILLISECONDS), self, CheckTimeout)

  val selectionUrls: Map[Int, String] = (cluster.nodes.indices zip Cluster.selectionUrls(cluster).map(_._2)).toMap

  log.info("selections are: {}", selectionUrls)

  // cluster membership is static
  def resolve(node: Int): ActorSelection = context.actorSelection(selectionUrls(node % cluster.nodes.size))
}

case class Node(id: Int, host: String, clientPort: Int, nodePort: Int)

case class Cluster(name: String, folder: String, retained: Int, nodes: Seq[Node]) {
  val nodeMap = nodes.map { n => (n.id, n) }.toMap
}

object Cluster {
  def senders(system: ActorSystem, nodes: Seq[Node]): Map[Int, ActorRef] = {
    val log = Logging.getLogger(system, this)
    log.info("creating senders for nodes {}", nodes)
    nodes.map { n =>
      n.id -> system.actorOf(Props(classOf[UdpSender], new java.net.InetSocketAddress(n.host, n.nodePort)), s"Sender${n.id}")
    }.toMap
  }

  def parseConfig(config: Config): Cluster = {
    val folder = config.getString("trex.data-folder")
    val name = config.getString("trex.cluster.name")
    val retained = config.getInt("trex.data-retained")
    val nodeIds: Array[String] = config.getString("trex.cluster.nodes").split(',')
    val nodes = nodeIds map { nodeId =>
      val host = config.getString(s"trex.cluster.node-$nodeId.host")
      val cport = config.getString(s"trex.cluster.node-$nodeId.client-port")
      val nport = config.getString(s"trex.cluster.node-$nodeId.node-port")
      Node(nodeId.toInt, host, cport.toInt, nport.toInt)
    }
    Cluster(name, folder, retained, collection.immutable.Seq(nodes: _*))
  }

  def selectionUrls(cluster: Cluster) =
    cluster.nodes.map { node =>
      (node.id, s"akka.tcp://${cluster.name}@${node.host}:${node.clientPort}/user/PaxosActor")
    }

}

class TypedActorPaxosEndpoint(config: PaxosActor.Configuration, broadcastReference: ActorRef, nodeUniqueId: Int, journal: Journal, target: AnyRef)
  extends PaxosActorWithTimeout(config, nodeUniqueId, broadcastReference, journal) {

  def broadcast(msg: Any): Unit = send(broadcastReference, msg)

  override val deliverClient: PartialFunction[CommandValue, AnyRef] = {
    case ClientRequestCommandValue(id, bytes) =>
      val mc@TypedActor.MethodCall(method, parameters) = TrexExtension(context.system).deserialize(bytes)
      log.debug("delivering {}", mc)
      val result = Try {
        val response = Option(method.invoke(target, parameters: _*))
        log.debug(s"invoked ${method.getName} returned $response")
        ServerResponse(id, response)
      } recover {
        case ex =>
          log.error(ex, s"call to $method with $parameters got exception $ex")
          ServerResponse(id, Option(ex))
      }
      result.get
  }
}

class TrexServer(cluster: Cluster, config: PaxosActor.Configuration, nodeUniqueId: Int, journal: Journal, target: AnyRef) extends Actor with ActorLogging {
  val selfNode = cluster.nodeMap(nodeUniqueId)

  val peerNodes = cluster.nodes.filterNot(_.id == nodeUniqueId)

  val peers: Map[Int, ActorRef] = Cluster.senders(context.system, peerNodes)

  val broadcast = context.system.actorOf(Props(classOf[Broadcast], peers.values.toSeq), "broadcast")

  val targetActor = context.system.actorOf(Props(classOf[TypedActorPaxosEndpoint], config, broadcast, nodeUniqueId, journal, target), "PaxosActor")

  val listener = context.system.actorOf(Props(classOf[UdpListener], new InetSocketAddress(selfNode.host, selfNode.nodePort), self), "UdpListener")

  override def receive: Receive = {
    case outbound: AnyRef if sender == targetActor =>
      route(outbound) ! outbound
    case inbound: AnyRef if sender == listener =>
      targetActor ! inbound
    case t: Terminated =>
      // TODO handle this
      log.warning(s"Termination notice $t")
    case unknown =>
      log.warning("{} unknown message {} from {}", this.getClass.getCanonicalName, unknown, sender())
  }

  def route(msg: AnyRef): ActorRef = {
    val nodeId = msg match {
      case acceptResponse: AcceptResponse =>
        acceptResponse.requestId.from
      case prepareResponse: PrepareResponse =>
        prepareResponse.requestId.from
      case retransmitResponse: RetransmitResponse =>
        retransmitResponse.to
      case retransmitRequest: RetransmitRequest =>
        retransmitRequest.to
      case x =>
        log.error("unknown message {}", x)
        0
    }
    log.debug("routing to {} message {}", nodeId, msg)
    peers(nodeId)
  }
}