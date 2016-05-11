package com.github.trex_paxos.internals

import java.security.SecureRandom

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable}
import PaxosActor._
import com.github.trex_paxos.library._
import com.typesafe.config.Config

import scala.collection.immutable.{SortedMap, TreeMap}
import scala.collection.mutable
import scala.util.Try

/**
 * Note that for testing this class does not schedule and manage its own timeouts. Use the subclass which schedules
 * its timeout rather than this baseclass.
 *
 * @param config Configuration such as timeout durations.
 * @param nodeUniqueId The unique identifier of this node. This *must* be unique in the cluster which is required as of the Paxos algorithm to work properly and be safe.
 * @param journal The durable journal required to store the state of the node in a stable manner between crashes.
 */
abstract class PaxosActorNoTimeout(config: PaxosProperties, val nodeUniqueId: Int, val journal: Journal) extends Actor
with PaxosIO
with ActorLogging
with AkkaLoggingAdapter {
  log.info("timeout min {}, timeout max {}", config.leaderTimeoutMin, config.leaderTimeoutMax)

  def clusterSize: Int

  var paxosAgent = new PaxosAgent(nodeUniqueId, Follower, PaxosData(journal.loadProgress(), 0, 0, clusterSize _,
    SortedMap.empty[Identifier, Map[Int, PrepareResponse]](Ordering.IdentifierLogOrdering), None,
    SortedMap.empty[Identifier, AcceptResponsesAndTimeout](Ordering.IdentifierLogOrdering),
    Map.empty[Identifier, (CommandValue, String)]))

  val logger = this

  private val paxosAlgorithm = new PaxosAlgorithm

  protected val actorRefWeakMap = new mutable.WeakHashMap[String,ActorRef]

  // for the algorithm to have no dependency on akka we need to assign a String IDs
  // to pass into the algorithm then later resolve the ActorRef by ID
  override def senderId: String = {
    val ref = sender()
    val pathAsString = ref.path.toString
    actorRefWeakMap.put(pathAsString, ref)
    logger.debug("weak map key {} value {}", pathAsString, ref)
    pathAsString
  }

  def respond(pathAsString: String, data: Any) = actorRefWeakMap.get(pathAsString) match {
    case Some(ref) =>
      ref ! data
    case _ =>
      logger.debug("weak map does not hold key {} to reply with {}", pathAsString, data)
  }

  override def receive: Receive = {
    case m: PaxosMessage =>
      val event = new PaxosEvent(this, paxosAgent, m)
      val agent = paxosAlgorithm(event)
      trace(event, sender().toString(), sent)
      transmit(sender())
      paxosAgent = agent
    case f => logger.error("Received unknown messages type ", f)
  }

  val minPrepare = Prepare(Identifier(nodeUniqueId, BallotNumber(Int.MinValue, Int.MinValue), Long.MinValue))

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

  def event(io: PaxosIO, stateName: PaxosRole, data: PaxosData, msg: PaxosMessage): PaxosEvent =
    PaxosEvent(io, PaxosAgent(nodeUniqueId, stateName, data), msg)

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

  def randomTimeout = freshTimeout(randomInterval)

  type Epoch = Option[BallotNumber]
  type PrepareSelfVotes = SortedMap[Identifier, Option[Map[Int, PrepareResponse]]]

  def trace(event: PaxosEvent, sender: String, sent: collection.immutable.Seq[PaxosMessage]): Unit = {}

  /**
   * The deliver method is called when a command is committed after having been selected by consensus.
 *
   * @param payload The selected value and a delivery id that can be used to deduplicate deliveries during crash recovery.
   * @return The response to the value command that has been delivered. May be an empty array.
   */
  def deliver(payload: Payload): Any = (filteredDeliverClient orElse deliverMembership)(payload)

  /**
   * The consensus algorithm my commit noop values which are filtered out rather than being passed to the client code.
   */
  val filteredDeliverClient: PartialFunction[Payload, Any] = {
    case Payload(_, NoOperationCommandValue) => NoOperationCommandValue.bytes
    case p => deliverClient(p)
  }

  /**
   * The cluster membership finite state machine. The new membership has been chosen but will come into effect
   * only for the next message for which we generate an accept message.
   */
  val deliverMembership: PartialFunction[Payload, Any] = {
    case Payload(_, m@MembershipCommandValue(_, members)) =>
      throw new AssertionError("not yet implemented")
  }

  /**
   * Notifies clients that it is no longer the leader by sending them an exception.
   */
  def sendNoLongerLeader(clientCommands: Map[Identifier, (CommandValue, String)]): Unit = clientCommands foreach {
    case (id, (cmd, client)) =>
      log.warning("Sending NoLongerLeader to client {} the outcome of the client cmd {} at slot {} is unknown.", client, cmd, id.logIndex)
      respond(client, new LostLeadershipException(nodeUniqueId, cmd.msgId))
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
abstract class PaxosActor(config: PaxosProperties, nodeUniqueId: Int, journal: Journal)
  extends PaxosActorNoTimeout(config, nodeUniqueId, journal) {

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

  def heartbeatInterval = config.leaderTimeoutMin / 4

  val leaderHeartbeat: Cancellable = { // TODO possibly a var and some callbacks on state changes to canel and restart
    log.info("Node {} setting heartbeat interval to {}", nodeUniqueId, heartbeatInterval)
    context.system.scheduler.schedule(Duration(5, MILLISECONDS), Duration(heartbeatInterval, MILLISECONDS), self, HeartBeat)
  }
}

object PaxosProperties {
  def apply(config: Config) = {
    /**
      * To ensure cluster stability you *must* test your max GC under extended peak load and set this as some multiple
      * of observed GC pause.
      */
    val leaderTimeoutMin = Try {
      config.getInt(leaderTimeoutMinKey)
    } getOrElse (1000)

    val leaderTimeoutMax = Try {
      config.getInt(leaderTimeoutMaxKey)
    } getOrElse (3 * leaderTimeoutMin)


    require(leaderTimeoutMax > leaderTimeoutMin)

    new PaxosProperties(leaderTimeoutMin, leaderTimeoutMax)
  }

  def apply() = new PaxosProperties(1000, 3000)
}

case class PaxosProperties(val leaderTimeoutMin: Long, val leaderTimeoutMax: Long)

object PaxosActor {

  val leaderTimeoutMinKey = "trex.leader-timeout-min"
  val leaderTimeoutMaxKey = "trex.leader-timeout-max"

  val random = new SecureRandom

  case class TraceData(ts: Long, nodeUniqueId: Int, stateName: PaxosRole, statData: PaxosData, sender: String, message: Any, sent: Seq[PaxosMessage])

  type Tracer = TraceData => Unit

  val freshAcceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout] = SortedMap.empty[Identifier, AcceptResponsesAndTimeout](Ordering.IdentifierLogOrdering)

  val minJournalBounds = JournalBounds(Long.MinValue, Long.MinValue)
}

