package com.github.trex_paxos.internals

import java.security.SecureRandom

import akka.actor.{Actor, ActorLogging, ActorRef}
import PaxosActor._
import com.github.trex_paxos.library._
import com.typesafe.config.Config

import scala.collection.immutable.{SortedMap, TreeMap}
import scala.collection.mutable
import scala.util.Try

/**
A concrete reference to a remote client.
*/
//case class ActorRemoteRef(val ref: ActorRef) extends RemoteRef

/**
 * Note that for testing this class does not schedule and manage its own timeouts. Extend a subclass which schedules
 * its timeout rather than this baseclass.
 *
 * @param config Configuration such as timeout durations and the cluster size.
 * @param nodeUniqueId The unique identifier of this node. This *must* be unique in the cluster which is required as of the Paxos algorithm to work properly and be safe.
 * @param broadcastRef An ActorRef through which the current cluster can be messaged.
 * @param journal The durable journal required to store the state of the node in a stable manner between crashes.
 */
abstract class PaxosActor(config: Configuration, val nodeUniqueId: Int, broadcastRef: ActorRef, val journal: Journal) extends Actor
with PaxosIO
with ActorLogging
with AkkaLoggingAdapter {
  log.info("timeout min {}, timeout max {}", config.leaderTimeoutMin, config.leaderTimeoutMax)

  var paxosAgent = new PaxosAgent(nodeUniqueId, Follower, PaxosData(journal.load(), 0, 0, config.clusterSize, SortedMap.empty[Identifier, Map[Int, PrepareResponse]](Ordering.IdentifierLogOrdering), None, SortedMap.empty[Identifier, AcceptResponsesAndTimeout](Ordering.IdentifierLogOrdering), Map.empty[Identifier, (CommandValue, String)]))

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
      trace(event, sender().toString())
      val agent = paxosAlgorithm(event)
      transmit(sender())
      paxosAgent = agent
  }

  val minPrepare = Prepare(Identifier(nodeUniqueId, BallotNumber(Int.MinValue, Int.MinValue), Long.MinValue))

  var sent: Seq[PaxosMessage] = Seq()

  def send(msg: PaxosMessage): Unit = {
    sent = sent :+ msg
  }

  def transmit(sender: ActorRef): Unit = {
    this.sent foreach {
      case m@(_: RetransmitRequest | _: RetransmitResponse | _: AcceptResponse | _: PrepareResponse) => send(sender, m)
      case m => broadcast(m)
    }
    this.sent = Seq()
  }

  def broadcast(msg: PaxosMessage): Unit = broadcastRef ! msg

  // tests can override this
  def clock() = {
    System.currentTimeMillis()
  }

  def highestAcceptedIndex = journal.bounds.max

  def event(io: PaxosIO, stateName: PaxosRole, data: PaxosData, msg: PaxosMessage): PaxosEvent = PaxosEvent(io, PaxosAgent(nodeUniqueId, stateName, data), msg)

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

  def trace(event: PaxosEvent, sender: String): Unit = {}

  /**
   * The deliver method is called when the value is committed.
   * @param value The committed value command to deliver.
   * @return The response to the value command that has been delivered. May be an empty array.
   */
  def deliver(value: CommandValue): Any = (deliverClient orElse deliverMembership)(value)

  /**
   * The cluster membership finite state machine. The new membership has been chosen but will come into effect
   * only for the next message for which we generate an accept message.
   */
  val deliverMembership: PartialFunction[CommandValue, Array[Byte]] = {
    case m@MembershipCommandValue(_, members) =>
      throw new AssertionError("not yet implemented")
  }

  /**
   * Notifies clients that it is no longer the leader by sending them an exception.
   */
  def sendNoLongerLeader(clientCommands: Map[Identifier, (CommandValue, String)]): Unit = clientCommands foreach {
    case (id, (cmd, client)) =>
      log.warning("Sending NoLongerLeader to client {} the outcome of the client cmd {} at slot {} is unknown.", client, cmd, id.logIndex)
      respond(client, new NoLongerLeaderException(nodeUniqueId, cmd.msgId))
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
  val deliverClient: PartialFunction[CommandValue, AnyRef]

}

/**
 * For testability the timeout behavior is not part of the baseclass
 * This class reschedules a random interval CheckTimeout used to timeout on responses and an evenly spaced Paxos.HeartBeat which is used by a leader.
 */
abstract class PaxosActorWithTimeout(config: Configuration, nodeUniqueId: Int, broadcast: ActorRef, journal: Journal)
  extends PaxosActor(config, nodeUniqueId, broadcast, journal) {

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

  val leaderHeartbeat = {
    log.info("Node {} setting heartbeat interval to {}", nodeUniqueId, heartbeatInterval)
    context.system.scheduler.schedule(Duration(5, MILLISECONDS), Duration(heartbeatInterval, MILLISECONDS), self, HeartBeat)
  }
}

object PaxosActor {

  val leaderTimeoutMinKey = "trex.leader-timeout-min"
  val leaderTimeoutMaxKey = "trex.leader-timeout-max"
  val fixedClusterSize = "trex.cluster-size"

  class Configuration(config: Config, val clusterSize: Int) {
    /**
     * You *must* test your max GC under extended peak load and set this as some multiple of observed GC pause to ensure cluster stability.
     */
    val leaderTimeoutMin = Try {
      config.getInt(leaderTimeoutMinKey)
    } getOrElse (1000)

    val leaderTimeoutMax = Try {
      config.getInt(leaderTimeoutMaxKey)
    } getOrElse (3 * leaderTimeoutMin)

    require(leaderTimeoutMax > leaderTimeoutMin)
  }

  object Configuration {
    def apply(config: Config, clusterSize: Int) = new Configuration(config, clusterSize)
  }

  val random = new SecureRandom

  // Log the nodeUniqueID, stateName,.underlyingActor.data. sender and message for tracing purposes
  case class TraceData(ts: Long, nodeUniqueId: Int, stateName: PaxosRole, statData: PaxosData, sender: String, message: Any)

  type Tracer = TraceData => Unit

  val freshAcceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout] = SortedMap.empty[Identifier, AcceptResponsesAndTimeout](Ordering.IdentifierLogOrdering)

  val minJournalBounds = JournalBounds(Long.MinValue, Long.MinValue)
}

