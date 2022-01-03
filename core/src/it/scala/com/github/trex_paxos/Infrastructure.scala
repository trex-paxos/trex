package com.github.trex_paxos

import java.io.FileWriter
import java.util.concurrent.CopyOnWriteArrayList

import akka.actor._
import com.github.trex_paxos.internals.PaxosActor.TraceData
import com.github.trex_paxos.internals.{PaxosActor, PaxosProperties}
import com.github.trex_paxos.library._
import com.typesafe.config.Config

import scala.annotation.tailrec
import scala.collection.immutable.{Seq, SortedMap}
import scala.collection.mutable
import scala.language.postfixOps
import scala.util.Try

class TestJournal extends Journal {
  private val _progress = Box(Journal.minBookwork.copy())
  private val _map = Box(SortedMap[Long, Accept]())

  def saveProgress(progress: Progress): Unit = _progress(progress)

  def loadProgress(): Progress = _progress()

  def accept(accepted: Accept*): Unit = accepted foreach { a =>
    _map(_map() + (a.id.logIndex -> a))
  }

  def accepted(logIndex: Long): Option[Accept] = _map().get(logIndex)

  def bounds(): JournalBounds = {
    val keys = _map().keys
    if (keys.isEmpty) JournalBounds(0L, 0L) else JournalBounds(keys.head, keys.last)
  }
}

class TestPaxosActor(config: PaxosProperties, clusterSizeF: () => Int, nodeUniqueId: Int, broadcastRef: ActorRef, journal: Journal, delivered: mutable.Buffer[Payload], tracer: Option[PaxosActor.Tracer])
  extends PaxosActor(config, nodeUniqueId, journal) {

  override def broadcast(msg: PaxosMessage): Unit = send(broadcastRef, msg)

  // does nothing but makes this class concrete for testing
  val deliverClient: PartialFunction[Payload, AnyRef] = {
    case Payload(_, ClientCommandValue(_, bytes)) => bytes
    case x => throw new IllegalArgumentException(x.toString)
  }

  override def deliver(payload: Payload): Array[Byte] = {
    delivered.append(payload)
    log.info("Node ")
    payload.command match {
      case ClientCommandValue(_, bytes) =>
        if (bytes.length > 0) Array[Byte]((-bytes(0)).toByte) else bytes
      case noop@NoOperationCommandValue => noop.bytes
    }
  }

  // custom heartbeat interval as things are all in memory
  override def heartbeatInterval = 33

  override def trace(event: PaxosEvent, sender: String, sent: Seq[PaxosMessage]): Unit = tracer.foreach(t => t(TraceData(System.nanoTime(), nodeUniqueId, event.agent.role, event.agent.data, sender, event.message, sent)))

  override def clusterSize: Int = clusterSizeF()

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
  // last leader used mainly so that we can kill the first responding leader than not send any other messages to it
  val lastRespondingLeader: Box[ActorRef] = new Box(None)
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
    import scala.jdk.CollectionConverters._
    val deliver: mutable.Buffer[Payload] = (new CopyOnWriteArrayList[Payload]).asScala
    delivered(delivered() + (i -> deliver))
    val actor: ActorRef = context.actorOf(Props(classOf[TestPaxosActor], PaxosProperties(config), () => size, i, self, node, deliver, Some(recordTraceData _)))
    children(children() + (i -> actor))
    log.info(s"$i -> $actor")
    lastRespondingLeader(actor)
    tracedData(tracedData() + (i -> Seq.empty))
  }

  val invertedChildren = children().map(_.swap)

  val roundRobinCounter = Box(0)

  @tailrec
  private def next(): ActorRef = {
    roundRobinCounter(roundRobinCounter() + 1)
    val actor = children()(roundRobinCounter() % children().size)
    killedNodes().contains(actor) match {
      case true => next()
      case _ => actor
    }
  }

  val valueByMsgId = Box(Map[String, ClientCommandValue]())

  def receive: Receive = {
    /**
      * Spray a client request at any node in the cluster
     */
    case r@ClientCommandValue(msgId, bytes) =>
      r.bytes(0) match {
        case b if b > 0 => // value greater than zero is client request send to leader
          valueByMsgId(valueByMsgId() + (r.msgUuid -> r))
          valuesToClients(valuesToClients() + ((-b).toByte -> sender()))
          val guessedLeader = next()
          log.info("ClusterHarness client rq: {} -> {} {}", bytes, invertedChildren(guessedLeader), guessedLeader)
          guessedLeader ! r
        case b =>
          assert(false)
      }

    /**
      * Got a response from a leader. Recorded who this leader is so we may kill it if asked to.
    */
    case response: Array[Byte] =>
      response(0) match {
        case b if b < 0 => // value less than zero is the committed response send back to client
          lastRespondingLeader(sender())
          log.info(s"ClusterHarness client rs: from {}", invertedChildren(sender()))
          valuesToClients()(b) ! response
          valuesToClients(valuesToClients() - b)
        case b =>
          assert(false)
      }

    /**
      * The node we guessed to send the client request says it is not the leader so spray the request at the next node.
      */
    case NotLeader(from, msgId) =>
      val guessedLeader = next()
      val v@ClientCommandValue(_, bytes) = valueByMsgId()(msgId)
      log.info("ClusterHarness NotLeader {} trying {} to {}", from, bytes(0), invertedChildren(guessedLeader))
      context.system.scheduler.scheduleOnce(10 millis, guessedLeader, v)

    /**
      * Kill the last responding leader. Make a note of who we killed so that we don't send any more messages to them.
      * Naturally a real client would not know who was alive or dead and would have to timeout on the request to a dead node.
      */
    case "KillLeader" =>
      log.info(s"ClusterHarness killing leader {}", invertedChildren(lastRespondingLeader()))
      lastRespondingLeader() ! PoisonPill.getInstance
      killedNodes(killedNodes() + lastRespondingLeader())

    /**
      * A node that we sent to have lost a leadership election due to stalls so we forward that onto the client.
      */
    case nlle: LostLeadershipException =>
      log.info("ClusterHarness got {} with valueByMsgId {} and valuesToClients {}", nlle, this.valueByMsgId, this.valuesToClients)
      val value = this.valueByMsgId()(nlle.msgId)
      value.bytes.headOption match {
        case Some(b) =>
          val client = this.valuesToClients()((-b).toByte)
          client ! nlle
        case _ => throw new AssertionError(s"ClusterHarness should be unreachable value=$value")
      }

    /**
      * Intra-Cluster messages are broadcast to all nodes including the possibly killed node
      */

    case p: Prepare =>
      log.info(s"ClusterHarness ${sender()} sent $p broadcasting")
      children() foreach {
        case (id, actor) if id != p.id.from =>
          log.info(s"$id <- $p : $actor <- ${sender()}")
          actor ! p
        case _ =>
      }
    case p: PrepareResponse =>
      children() foreach {
        case (id, actor) if id == p.to =>
          log.info(s"ClusterHarness $id <- $p : $actor <- ${sender()}")
          actor ! p
        case _ =>
      }
    case a: Accept =>
      children() foreach {
        case (id, actor) if id != a.id.from =>
          log.info(s"ClusterHarness $id <- $a : $actor <- ${sender()}")
          actor ! a
        case _ =>
      }
    case a: AcceptResponse =>
      children() foreach {
        case (id, actor) if id == a.to =>
          log.info(s"ClusterHarness $id <- $a : $actor <- ${sender()}")
          actor ! a
        case _ =>
      }
    case c: Commit =>
      children() foreach {
        case (id, actor) if id != c.identifier.from =>
          log.info(s"ClusterHarness $id <- $c : $actor <- ${sender()}")
          actor ! c
        case _ =>
      }
    case r@RetransmitRequest(_, to, _) =>
      children() foreach {
        case (id, actor) if to == id =>
          log.info(s"ClusterHarness $id <- $r : $actor <- ${sender()}")
          actor ! r
        case _ =>
      }
    case r: RetransmitResponse =>
      children() foreach {
        case (id, actor) if r.to == id =>
          log.info(s"ClusterHarness $id <- $r : $actor <- ${sender()}")
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

      log.info("ClusterHarness halting all nodes")
      children().values foreach {
        c => c ! PoisonPill.getInstance
      }
      self ! PoisonPill.getInstance

      // output what was committed
      sender() ! delivered()

    case m =>
      val msg =
      log.error(s"ClusterHarness unknown message $m")
      throw new IllegalArgumentException(m.getClass.getCanonicalName + " : " + m.toString)
  }
}
