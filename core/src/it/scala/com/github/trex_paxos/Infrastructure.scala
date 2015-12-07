package com.github.trex_paxos

import java.io.FileWriter
import java.util
import java.util.Collections
import java.util.concurrent.CopyOnWriteArrayList

import akka.actor._
import com.github.trex_paxos.internals.PaxosActor.TraceData
import com.github.trex_paxos.internals.{PaxosActorWithTimeout, PaxosActor}
import com.github.trex_paxos.library._
import com.typesafe.config.Config

import scala.collection.immutable.{Seq, SortedMap}
import scala.collection.mutable
import scala.concurrent.JavaConversions
import scala.language.postfixOps
import scala.util.Try


class TestJournal extends Journal {
  private val _progress = Box(Journal.minBookwork.copy())
  private val _map = Box(SortedMap[Long, Accept]())

  def save(progress: Progress): Unit = _progress(progress)

  def load(): Progress = _progress()

  def accept(accepted: Accept*): Unit = accepted foreach { a =>
    _map(_map() + (a.id.logIndex -> a))
  }

  def accepted(logIndex: Long): Option[Accept] = _map().get(logIndex)

  def bounds: JournalBounds = {
    val keys = _map().keys
    if (keys.isEmpty) JournalBounds(0L, 0L) else JournalBounds(keys.head, keys.last)
  }
}

class TestPaxosActorWithTimeout(config: PaxosActor.Configuration, nodeUniqueId: Int, broadcastRef: ActorRef, journal: Journal, delivered: mutable.Buffer[Payload], tracer: Option[PaxosActor.Tracer])
  extends PaxosActorWithTimeout(config, nodeUniqueId, broadcastRef, journal) {

  def broadcast(msg: Any): Unit = send(broadcastRef, msg)

  // does nothing but makes this class concrete for testing
  val deliverClient: PartialFunction[Payload, AnyRef] = {
    case Payload(_, ClientRequestCommandValue(_, bytes)) => bytes
    case x => throw new IllegalArgumentException(x.toString)
  }

  override def deliver(payload: Payload): Array[Byte] = {
    delivered.append(payload)
    payload.command match {
      case ClientRequestCommandValue(_, bytes) =>
        if (bytes.length > 0) Array[Byte]((-bytes(0)).toByte) else bytes
      case noop@NoOperationCommandValue => noop.bytes
    }
  }

  // custom heartbeat interval as things are all in memory
  override def heartbeatInterval = 33

  override def trace(event: PaxosEvent, sender: String, sent: Seq[PaxosMessage]): Unit = tracer.foreach(t => t(TraceData(System.nanoTime(), nodeUniqueId, event.agent.role, event.agent.data, sender, event.message, sent)))
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
  val children = Box(Map[Int, ActorRef]())
  // the journals
  val journal = Box(Map[Int, TestJournal]())
  // the values delivered to the application
  val delivered = Box(Map[Int, mutable.Buffer[Payload]]())
  // lookup of which client sent which data so we can route it back correctly
  val valuesToClients = Box(Map[Byte, ActorRef]())
  // last leader so we can kill it
  val lastLeader: Box[ActorRef] = new Box(None)
  // nodes that are no longer reachable
  val killedNodes = Box(Set[ActorRef]())
  // record what each node saw in which state for debugging a full trace
  val tracedData = Box(SortedMap[Int, Seq[TraceData]]())

  def recordTraceData(data: TraceData): Unit = {
    tracedData(tracedData() + (data.nodeUniqueId -> (tracedData()(data.nodeUniqueId) :+ data)))
  }

  (0 until size) foreach { i =>
    val node = new TestJournal
    journal(journal() + (i -> node))
    val deliver: mutable.Buffer[Payload] = collection.JavaConversions.asScalaBuffer(new CopyOnWriteArrayList[Payload])
    delivered(delivered() + (i -> deliver))
    val actor: ActorRef = context.actorOf(Props(classOf[TestPaxosActorWithTimeout], PaxosActor.Configuration(config, size), i, self, node, deliver, Some(recordTraceData _)))
    children(children() + (i -> actor))
    log.info(s"$i -> $actor")
    lastLeader(actor)
    tracedData(tracedData() + (i -> Seq.empty))
  }

  val invertedChildren = children().map(_.swap)

  val roundRobinCounter = Box(0)

  def nextRoundRobinNode(not: ActorRef) = {
    var actor = children()(roundRobinCounter() % children().size)
    roundRobinCounter(roundRobinCounter() + 1)
    while (actor == not || killedNodes().contains(actor)) {
      actor = children()(roundRobinCounter() % children().size)
      roundRobinCounter(roundRobinCounter() + 1)
    }
    actor
  }

  val valueByMsgId = Box(Map[Long, ClientRequestCommandValue]())

  def receive: Receive = {
    case r@ClientRequestCommandValue(msgId, bytes) =>
      r.bytes(0) match {
        case b if b > 0 => // value greater than zero is client request send to leader
          valueByMsgId(valueByMsgId() + (r.msgId -> r))
          valuesToClients(valuesToClients() + ((-b).toByte -> sender))
          val child = nextRoundRobinNode(sender)
          log.info("Test cluster forwarding client data {} to {}", bytes, invertedChildren(child))
          child ! r
        case b =>
          assert(false)
      }
    case response: Array[Byte] =>
      response(0) match {
        case b if b < 0 => // value less than zero is the committed response send back to client
          lastLeader(sender)
          log.info(s"committed response from {}", invertedChildren(lastLeader()))
          valuesToClients()(b) ! response
          valuesToClients(valuesToClients() - b)
        case b =>
          assert(false)
      }
    case NotLeader(from, msgId) =>
      val child = nextRoundRobinNode(sender)
      val v@ClientRequestCommandValue(_, bytes) = valueByMsgId()(msgId)
      log.info("NotLeader {} trying {} to {}", from, bytes(0), invertedChildren(child))
      context.system.scheduler.scheduleOnce(10 millis, child, v)
    case p: Prepare =>
      log.info(s"$sender sent $p broadcasting")
      children() foreach {
        case (id, actor) if id != p.id.from =>
          log.info(s"$id <- $p : $actor <- $sender")
          actor ! p
        case _ =>
      }
    case p: PrepareResponse =>
      children() foreach {
        case (id, actor) if id == p.requestId.from =>
          log.info(s"$id <- $p : $actor <- $sender")
          actor ! p
        case _ =>
      }
    case a: Accept =>
      children() foreach {
        case (id, actor) if id != a.id.from =>
          log.info(s"$id <- $a : $actor <- $sender")
          actor ! a
        case _ =>
      }
    case a: AcceptResponse =>
      children() foreach {
        case (id, actor) if id == a.requestId.from =>
          log.info(s"$id <- $a : $actor <- $sender")
          actor ! a
        case _ =>
      }
    case c: Commit =>
      log.info(s"$sender sent $c broadcasting")
      children() foreach {
        case (id, actor) if id != c.identifier.from =>
          log.info(s"$id <- $c : $actor <- $sender")
          actor ! c
        case _ =>
      }
    case r@RetransmitRequest(_, to, _) =>
      children() foreach {
        case (id, actor) if to == id =>
          log.info(s"$id <- $r : $actor <- $sender")
          actor ! r
        case _ =>
      }
    case r: RetransmitResponse =>
      children() foreach {
        case (id, actor) if r.to == id =>
          log.info(s"$id <- $r : $actor <- $sender")
          actor ! r
        case _ =>
      }
    case ClusterHarness.Halt =>

      val path = s"target/${System.currentTimeMillis()}.trex.log"
      val fw = new FileWriter(path, true)

      log.info(s"dumping state trace to $path")
      Try {
        tracedData().toSeq foreach {
          case (node, t: Seq[TraceData]) =>
            t foreach { d =>
              val TraceData(ts, id, state, data, sender, msg, sent) = d
              fw.write(s"$ts|$id|$state|$msg|$sent|$sender|$data\n")
            }
        }
      } match {
        case _ => fw.close()
      }

      log.info("halting all nodes")
      children().values foreach {
        c => c ! PoisonPill.getInstance
      }
      self ! PoisonPill.getInstance

      // output what was committed
      sender ! delivered()
    case "KillLeader" =>
      log.info(s"killing leader {}", invertedChildren(lastLeader()))
      lastLeader() ! PoisonPill.getInstance
      killedNodes(killedNodes() + lastLeader())
    case nlle: NoLongerLeaderException =>
      log.info("got {} with valueByMsgId {} and valuesToClients {}", nlle, this.valueByMsgId, this.valuesToClients)
      val value = this.valueByMsgId()(nlle.msgId)
      value.bytes.headOption match {
        case Some(b) =>
          val client = this.valuesToClients()((-b).toByte)
          client ! nlle
        case _ => throw new AssertionError(s"should be unreachable value=$value")
      }
    case m =>
      System.err.println(s"unknown message $m")
      throw new IllegalArgumentException(m.getClass.getCanonicalName)
  }
}
