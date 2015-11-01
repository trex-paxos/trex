package com.github.simbo1905.trex

import java.io.FileWriter

import akka.actor._
import com.github.simbo1905.trex.internals.PaxosActor.TraceData
import com.github.simbo1905.trex.internals._
import com.github.simbo1905.trex.library._
import com.typesafe.config.Config

import scala.collection.immutable.{Seq, SortedMap}
import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps
import scala.util.Try

class TestJournal extends Journal {
  private var _progress = Journal.minBookwork.copy()
  private var _map: SortedMap[Long, Accept] = SortedMap.empty

  def save(progress: Progress): Unit = _progress = progress

  def load(): Progress = _progress

  def accept(accepted: Accept*): Unit = accepted foreach { a =>
    _map = _map + (a.id.logIndex -> a)
  }

  def accepted(logIndex: Long): Option[Accept] = _map.get(logIndex)

  def bounds: JournalBounds = {
    val keys = _map.keys
    if (keys.isEmpty) JournalBounds(0L, 0L) else JournalBounds(keys.head, keys.last)
  }
}

class TestPaxosActorWithTimeout(config: PaxosActor.Configuration, nodeUniqueId: Int, broadcastRef: ActorRef, journal: Journal, delivered: ArrayBuffer[CommandValue], tracer: Option[PaxosActor.Tracer])
  extends PaxosActorWithTimeout(config, nodeUniqueId, broadcastRef, journal) {

  def broadcast(msg: Any): Unit = send(broadcastRef, msg)

  // does nothing but makes this class concrete for testing
  val deliverClient: PartialFunction[CommandValue, AnyRef] = {
    case ClientRequestCommandValue(_, bytes) => bytes
  }

  override def deliver(value: CommandValue): Array[Byte] = {
    delivered.append(value)
    value match {
      case ClientRequestCommandValue(_, bytes) =>
        if (bytes.length > 0) Array[Byte]((-bytes(0)).toByte) else bytes
      case noop@NoOperationCommandValue => noop.bytes
    }
  }

  // custom heartbeat interval as things are all in memory
  override def heartbeatInterval = 33

  override def trace(state: PaxosRole, data: PaxosData, msg: Any): Unit = tracer.foreach(t => t(TraceData(nodeUniqueId, state, data, None, msg)))
}

object ClusterHarness {
  type Trace = (Array[Byte]) => Array[Byte]

  case object Halt

}

/**
 * Synthetically simulates the network between actors and allows us to interfere with nodes (e.g. crash, suspend, ...)
 *
 * @param size The cluster size
 * @param config The cluster config
 */
class ClusterHarness(val size: Int, config: Config) extends Actor with ActorLogging {

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  // the paxos actor nodes in our cluster
  var children = Map.empty[Int, ActorRef]
  // the journals
  var journal = Map.empty[Int, TestJournal]
  // the values delivered to the application
  var delivered = Map.empty[Int, ArrayBuffer[CommandValue]]
  // lookup of which client sent which data so we can route it back correctly
  var valuesToClients = Map.empty[Byte, ActorRef]
  // last leader so we can kill it
  var lastLeader: ActorRef = null
  // nodes that are no longer reachable
  var killedNodes = Set.empty[ActorRef]
  // record what each node saw in which state for debugging a full trace
  var tracedData = SortedMap.empty[Int, Seq[TraceData]]

  def recordTraceData(data:TraceData): Unit = {
    tracedData = tracedData + (data.nodeUniqueId -> (tracedData(data.nodeUniqueId) :+ data))
  }

  (0 until size) foreach { i =>
    val node = new TestJournal
    journal = journal + (i -> node)
    val deliver: ArrayBuffer[CommandValue] = ArrayBuffer.empty
    delivered = delivered + (i -> deliver)
    val actor: ActorRef = context.actorOf(Props(classOf[TestPaxosActorWithTimeout], PaxosActor.Configuration(config, size), i, self, node, deliver, Some(recordTraceData _)))
    children = children + (i -> actor)
    log.info(s"$i -> $actor")
    lastLeader = actor
    tracedData = tracedData + (i -> Seq.empty)
  }

  val invertedChildren = children.map(_.swap)

  var roundRobinCounter = 0

  def nextRoundRobinNode(not: ActorRef) = {
    var actor = children(roundRobinCounter % children.size)
    roundRobinCounter = roundRobinCounter + 1
    while (actor == not || killedNodes.contains(actor)) {
      actor = children(roundRobinCounter % children.size)
      roundRobinCounter = roundRobinCounter + 1
    }
    actor
  }

  var valueByMsgId: Map[Long, ClientRequestCommandValue] = Map.empty

  def receive: Receive = {
    case r@ClientRequestCommandValue(msgId, bytes) =>
      r.bytes(0) match {
        case b if b > 0 => // value greater than zero is client request send to leader
          valueByMsgId = valueByMsgId + (r.msgId -> r)
          valuesToClients = valuesToClients + ((-b).toByte -> sender)
          val child = nextRoundRobinNode(sender)
          log.info("Test cluster forwarding client data {} to {}", bytes, invertedChildren(child))
          child ! r
        case b =>
          assert(false)
      }
    case response: Array[Byte] =>
      response(0) match {
        case b if b < 0 => // value less than zero is the committed response send back to client
          lastLeader = sender
          log.info(s"committed response from {}", invertedChildren(lastLeader))
          valuesToClients(b) ! response
          valuesToClients = valuesToClients - b
        case b =>
          assert(false)
      }
    case NotLeader(from, msgId) =>
      val child = nextRoundRobinNode(sender)
      val v@ClientRequestCommandValue(_, bytes) = valueByMsgId(msgId)
      log.info("Test cluster resending {} to node {}", bytes(0), invertedChildren(child))
      context.system.scheduler.scheduleOnce(10 millis, child, v)
    case p: Prepare =>
      log.info(s"$sender sent $p broadcasting")
      children foreach {
        case (id, actor) if id != p.id.from =>
          actor ! p
        case _ =>
      }
    case p: PrepareResponse =>
      children foreach {
        case (id, actor) if id == p.requestId.from =>
          log.info(s"$sender sent $p to $actor")
          actor ! p
        case _ =>
      }
    case a: Accept =>
      children foreach {
        case (id, actor) if id != a.id.from =>
          log.info(s"$sender sent $a to $actor")
          actor ! a
        case _ =>
      }
    case a: AcceptResponse =>
      children foreach {
        case (id, actor) if id == a.requestId.from =>
          log.info(s"$sender sent $a to $actor")
          actor ! a
        case _ =>
      }
    case c: Commit =>
      log.info(s"$sender sent $c broadcasting")
      children foreach {
        case (id, actor) if id != c.identifier.from =>
          actor ! c
        case _ =>
      }
    case r@RetransmitRequest(_, to, _) =>
      children foreach {
        case (id, actor) if to == id =>
          log.info(s"$sender sent $r to $actor")
          actor ! r
        case _ =>
      }
    case r: RetransmitResponse =>
      log.info(s"$sender sent $r to ${r.to}")
      children foreach {
        case (id, actor) if r.to == id =>
          actor ! r
        case _ =>
      }
    case ClusterHarness.Halt =>

      val path = s"target/${System.currentTimeMillis()}.trex.log"
      val fw = new FileWriter(path, true)

      log.info(s"dumping state trace to $path")
      Try {
        tracedData.toSeq foreach {
          case (node, t: Seq[TraceData]) =>
            fw.write(s"#node $node\n\n")
            t foreach { d =>
              val TraceData(nId, state, data, sender, msg) = d
              sender match {
                case Some(actorRef) =>
                  // if the test fails to commit all values then the leader will have
                  // a reference to the client who sent the message which does not pickle.
                  // since the test harness sent the message we simply scrub that field so that
                  // we can log the state of the leader when the test fails due to a failed commit.
                  val dataNoActors = data.copy(clientCommands = Map.empty)
                  fw.write(s"$state|$dataNoActors|${actorRef.toString}|$msg\n\n")
                case None =>
                  fw.write(s"delivered: $msg\n\n")
              }
            }
        }
      } match {
        case _ => fw.close()
      }

      log.info("halting all nodes")
      children.values foreach {
        c => c ! PoisonPill.getInstance
      }
      self ! PoisonPill.getInstance
    case "KillLeader" =>
      log.info(s"killing leader {}", invertedChildren(lastLeader))
      lastLeader ! PoisonPill.getInstance
      killedNodes = killedNodes + lastLeader
    case nlle: NoLongerLeaderException =>
      log.info("got {} with valueByMsgId {} and valuesToClients {}", nlle, this.valueByMsgId, this.valuesToClients)
      val value = this.valueByMsgId(nlle.msgId)
      val b = value.bytes.head
      val client = this.valuesToClients((-b).toByte)
      client ! nlle
    case m =>
      System.err.println(s"unknown message $m")
      throw new IllegalArgumentException(m.getClass.getCanonicalName)
  }
}
