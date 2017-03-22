package com.github.trex_paxos.core

import java.security.SecureRandom

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable}
import AkkaPaxosActor._
import com.github.trex_paxos.PaxosProperties
import com.github.trex_paxos.library._
import com.typesafe.config.Config

import scala.collection.immutable.{SortedMap, TreeMap}
import scala.collection.{Map, mutable}
import scala.util.Try

/**
 * Note that for testing this class does not schedule and manage its own timeouts. Use the subclass which schedules
 * its timeout rather than this baseclass.
 *
 * @param config Configuration such as timeout durations.
 * @param nodeUniqueId The unique identifier of this node. This *must* be unique in the cluster which is required as of the Paxos algorithm to work properly and be safe.
 * @param journal The durable journal required to store the state of the node in a stable manner between crashes.
 */
abstract class AkkaPaxosActorNoTimeout(config: PaxosProperties, val nodeUniqueId: Int, val journal: Journal) extends Actor
with PaxosIO
with ActorLogging
with AkkaLoggingAdapter {
  log.info("timeout min {}, timeout max {}", config.leaderTimeoutMin, config.leaderTimeoutMax)

  def clusterSize: Int

  var paxosAgent = initialAgent(nodeUniqueId, journal.loadProgress(), clusterSize _)

  val logger = this

  private val paxosAlgorithm = new PaxosAlgorithm

  protected val actorRefWeakMap = new mutable.WeakHashMap[Identifier,(ActorRef, CommandValue)]

  /**
    * Associate a command value with a paxos identifier so that the result of the commit can be routed back to the sender of the command
    *
    * @param value The command to asssociate with a message identifer.
    * @param id    The message identifier
    */
  override def associate(value: CommandValue, id: Identifier): Unit = actorRefWeakMap.put(id, (sender(), value))

  /**
    * Respond to clients.
    *
    * @param results The results of a fast forward commit or None if leadership was lost such that the commit outcome is unknown.
    */
   def respond(results: Option[scala.collection.immutable.Map[Identifier, Any]]): Unit = results match {
    case Some(results) =>

      val valueIds = results.keys.toSet

      val clientsMap: Map[Identifier, (ActorRef, CommandValue)] = actorRefWeakMap filter {
        case v@(id, _) if valueIds.contains(id) => true
        case _ => false
      }

      clientsMap foreach {
        case (id, (client, command)) =>
          client ! results(id)
          actorRefWeakMap.remove(id)
      }

    case None =>
      actorRefWeakMap foreach {
        case (id, (client, cmd)) => client ! LostLeadershipException(nodeUniqueId, cmd.msgUuid);
      }
      actorRefWeakMap.clear()

  }

  override def receive: Receive = {
    case m: PaxosMessage =>
      val event = new PaxosEvent(this, paxosAgent, m)
      val agent = paxosAlgorithm(event)
      trace(event, sender().toString(), sent)
      transmit(sender())
      paxosAgent = agent
    case f => logger.error("Received unknown messages type {}", f)
  }

  val minPrepare = Prepare(Identifier(nodeUniqueId, BallotNumber(0, 0), 0))

  var sent: collection.immutable.Seq[PaxosMessage] = collection.immutable.Seq()

  def send(msg: PaxosMessage): Unit = {
    sent = sent :+ msg
  }

  // FIXME this routing needs to be pulled out
  def transmit(sender: ActorRef): Unit = {
    this.sent foreach {
      case m@(_: RetransmitRequest | _: RetransmitResponse | _: AcceptResponse | _: PrepareResponse | _: NotLeader ) =>
        logger.debug("sending {} msg {}", sender, m)
        send(sender, m)
      case m =>
        logger.debug("broadcasting {}", m)
        broadcast(m)
    }
    this.sent = collection.immutable.Seq()
  }

  def broadcast(msg: PaxosMessage): Unit

  // tests can override this
  def clock() = {
    System.currentTimeMillis()
  }

  def highestAcceptedIndex = journal.bounds.max

  def randomInterval: Long = {
    config.leaderTimeoutMin + ((config.leaderTimeoutMax - config.leaderTimeoutMin) * random.nextDouble()).toLong
  }

  /**
   * Returns the next timeout put using a testable clock.
   */
  def freshTimeout(interval: Long): Long = {
    val t = clock() + interval
    t
  }

  def scheduleRandomCheckTimeout = freshTimeout(randomInterval)

  type Epoch = Option[BallotNumber]
  type PrepareSelfVotes = SortedMap[Identifier, Option[Map[Int, PrepareResponse]]]

  def trace(event: PaxosEvent, sender: String, sent: collection.immutable.Seq[PaxosMessage]): Unit = {}

  /**
   * The deliver method is called when a command is committed after having been selected by consensus.
   * @param payload The selected value and a delivery id that can be used to deduplicate deliveries during crash recovery.
   * @return The response to the value command that has been delivered. May be an empty array.
   */
  def deliver(payload: Payload): Any = (deliverClient orElse deliverMembership)(payload)

  /**
   * The cluster m finite state machine. The new m has been chosen but will come into effect
   * only for the next message for which we generate an accept message.
   */
  val deliverMembership: PartialFunction[Payload, Any] = {
    case Payload(_, _) =>
      throw new AssertionError("not yet implemented") // FIXME
  }

  /**
   * If you require transactions in the host application then you need to supply a custom Journal which participates
   * in your transactions. You also need to override this method to buffer the messages then either send them post commit
   * else delete them post rollback. Paxos is safe to lost messages so it is safe to crash after committing the journal
   * before having sent out the messages. Paxos is *not* safe to "forgotten outcomes" so it is never safe to send messages
   * when you rolled back your custom Journal.
   */
  def send(actor: ActorRef, msg: Any): Unit = {
    actor ! msg
  }

  /**
   * The host application finite state machine invocation.
   * This method is abstract as the implementation is specific to the host application.
   */
  val deliverClient: PartialFunction[Payload, AnyRef]

}

/**
 * This class reschedules a random interval CheckTimeout used to timeout on responses and an evenly spaced
 * Paxos.HeartBeat which is used by a leader.
 */
abstract class AkkaPaxosActor(config: PaxosProperties, nodeUniqueId: Int, journal: Journal)
  extends AkkaPaxosActorNoTimeout(config, nodeUniqueId, journal) {

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  private[this] def scheduleCheckTimeout(interval: Long) = context.system.scheduler.scheduleOnce(Duration(interval, MILLISECONDS), self, CheckTimeout)

  override def preStart() = scheduleCheckTimeout(randomInterval)

  // override postRestart so we don't call preStart and schedule a new CheckTimeout message
  override def postRestart(reason: Throwable) = {}

  // override the generator of the random timeout with a method which schedules the message to arrive soon after that
  override def freshTimeout(interval: Long): Long = {
    val timeout = super.freshTimeout(interval)
    scheduleCheckTimeout(interval)
    timeout
  }

  def heartbeatInterval = config.leaderTimeoutMin / 4 // TODO set this value by config else default

  val leaderHeartbeat: Cancellable = { // TODO possibly a var and some callbacks on state changes to canel and restart
    log.info("Node {} setting heartbeat interval to {}", nodeUniqueId, heartbeatInterval)
    context.system.scheduler.schedule(Duration(5, MILLISECONDS), Duration(heartbeatInterval, MILLISECONDS), self, HeartBeat)
  }
}

object AkkaPaxosActor {

  val leaderTimeoutMinKey = "trex.leader-timeout-min"
  val leaderTimeoutMaxKey = "trex.leader-timeout-max"

  val random = new SecureRandom

  case class TraceData(ts: Long, nodeUniqueId: Int, stateName: PaxosRole, statData: PaxosData, sender: String, message: Any, sent: Seq[PaxosMessage])

  type Tracer = TraceData => Unit

  val freshAcceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout] = SortedMap.empty[Identifier, AcceptResponsesAndTimeout](Ordering.IdentifierLogOrdering)

  val minJournalBounds = JournalBounds(0, 0)

  def initialAgent(nodeUniqueId: Int, progress: Progress, clusterSize: () => Int) =
    new PaxosAgent(nodeUniqueId, Follower, PaxosData(progress, 0, 0,
    SortedMap.empty[Identifier, scala.collection.immutable.Map[Int, PrepareResponse]](Ordering.IdentifierLogOrdering), None,
    SortedMap.empty[Identifier, AcceptResponsesAndTimeout](Ordering.IdentifierLogOrdering)
    ), DefaultQuorumStrategy(clusterSize))


}

