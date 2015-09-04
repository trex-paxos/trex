package com.github.simbo1905.trex.internals

import java.security.SecureRandom

import akka.actor.{ActorRef, FSM}
import com.github.simbo1905.trex._
import com.github.simbo1905.trex.internals.PaxosActor._
import com.typesafe.config.Config

import scala.annotation.elidable
import scala.collection.SortedMap
import scala.collection.immutable.TreeMap
import scala.util.Try

object Ordering {

  implicit object IdentifierLogOrdering extends Ordering[Identifier] {
    def compare(o1: Identifier, o2: Identifier) = if (o1.logIndex == o2.logIndex) 0 else if (o1.logIndex >= o2.logIndex) 1 else -1
  }

  implicit object BallotNumberOrdering extends Ordering[BallotNumber] {
    def compare(n1: BallotNumber, n2: BallotNumber) = if (n1 == n2) 0 else if (n1 > n2) 1 else -1
  }

}

/**
 * Paxos Actor Finite State Machine using immutable messages and immutable state. Note that for testing this class does
 * not schedule and manage its own timeouts. Extend a subclass which schedules its timeout rather than this baseclass.
 *
 * @param config Configuration such as timeout durations and the cluster size.
 * @param nodeUniqueId The unique identifier of this node. This *must* be unique in the cluster which is required as of the Paxos algorithm to work properly and be safe.
 * @param broadcaseRef An ActorRef through which the current cluster can be messaged.
 * @param journal The durable journal required to store the state of the node in a stable manner between crashes.
 */
abstract class PaxosActor(config: Configuration, val nodeUniqueId: Int, broadcaseRef: ActorRef, val journal: Journal) extends FSM[PaxosRole, PaxosData]
with RetransmitHandler
with ReturnToFollowerHandler
with UnhandledHandler
with CommitHandler
with ResendAcceptsHandler
with AcceptResponsesHandler
with PrepareResponseHandler
with FollowerTimeoutHandler
{

  import Ordering._

  val minPrepare = Prepare(Identifier(nodeUniqueId, BallotNumber(Int.MinValue, Int.MinValue), Long.MinValue))

  // tests can override this
  def clock() = {
    System.currentTimeMillis()
  }

  log.info("timeout min {}, timeout max {}", config.leaderTimeoutMin, config.leaderTimeoutMax)

  startWith(Follower, PaxosData(journal.load(), 0, 0, config.clusterSize))

  def journalProgress(progress: Progress) = {
    journal.save(progress)
    progress
  }

  /**
   * Processes both RetransmitRequest and RetransmitResponse. Used by all states.
   */
  val retransmissionStateFunction: StateFunction = {
    case e@Event(r@RetransmitResponse(from, to, committed, proposed), oldData@PaxosData(p@Progress(highestPromised, highestCommitted), _, _, _, _, _, _, _)) => // TODO extractors
      trace(stateName, e.stateData, sender, e.event)
      log.debug("Node {} RetransmitResponse with {} committed and {} proposed entries", nodeUniqueId, committed.size, proposed.size)
      stay using PaxosData.progressLens.set(oldData, handleRetransmitResponse(r, oldData))

    case e@Event(r: RetransmitRequest, oldData@HighestCommittedIndex(committedLogIndex)) =>
      trace(stateName, e.stateData, sender, e.event)
      log.debug("Node {} {} RetransmitRequest {} with high watermark {}", nodeUniqueId, stateName, r, committedLogIndex)
      handleRetransmitRequest(sender(), r, oldData)
      stay
  }

  val prepareStateFunction: StateFunction = {
    // nack a low prepare
    case e@Event(Prepare(id), data: PaxosData) if id.number < data.progress.highestPromised =>
      trace(stateName, e.stateData, sender, e.event)
      log.debug("Node {} {} nacking a low prepare {}", nodeUniqueId, stateName, id)
      send(sender, PrepareNack(id, nodeUniqueId, data.progress, highestAcceptedIndex, data.leaderHeartbeat))
      stay

    // ack a high prepare
    case e@Event(Prepare(id), data: PaxosData) if id.number > data.progress.highestPromised =>
      trace(stateName, e.stateData, sender, e.event)
      log.debug("Node {} {} acking higher prepare {}", nodeUniqueId, stateName, id)
      val newData = PaxosData.progressLens.set(data, journalProgress(Progress.highestPromisedLens.set(data.progress, id.number)))
      send(sender, PrepareAck(id, nodeUniqueId, data.progress, highestAcceptedIndex, data.leaderHeartbeat, journal.accepted(id.logIndex)))
      // higher promise we can no longer journal client values as accepts under our epoch so cannot commit and must backdown
      goto(Follower) using backdownData(newData)

    // ack repeated prepare
    case e@Event(Prepare(id), data: PaxosData) if id.number == data.progress.highestPromised =>
      trace(stateName, e.stateData, sender, e.event)
      log.debug("Node {} {} acking same prepare {}", nodeUniqueId, stateName, id)
      send(sender, PrepareAck(id, nodeUniqueId, data.progress, highestAcceptedIndex, data.leaderHeartbeat, journal.accepted(id.logIndex)))
      stay
  }

  val acceptStateFunction: StateFunction = {
    // nack lower accept
    case e@Event(Accept(id, _), data: PaxosData) if id.number < data.progress.highestPromised =>
      trace(stateName, e.stateData, sender, e.event)
      log.debug("Node {} {} nacking low accept {} as progress {}", nodeUniqueId, stateName, id, data.progress)
      send(sender, AcceptNack(id, nodeUniqueId, data.progress))
      stay

    // nack higher accept for slot which is committed
    case e@Event(Accept(id, slot), data: PaxosData) if id.number > data.progress.highestPromised && id.logIndex <= data.progress.highestCommitted.logIndex =>
      trace(stateName, e.stateData, sender, e.event)
      log.debug("Node {} {} nacking high accept {} as progress {}", nodeUniqueId, stateName, id, data.progress)
      send(sender, AcceptNack(id, nodeUniqueId, data.progress))
      stay

    // ack accept as high as promise. if id.number > highestPromised must update highest promised in progress http://stackoverflow.com/q/29880949/329496
    case e@Event(a@Accept(id, value), d@PaxosData(p@Progress(highestPromised, _), _, _, _, _, _, _, _)) if highestPromised <= id.number =>
      trace(stateName, e.stateData, sender, e.event)
      log.debug("Node {} {} acking accept {} as last promise {}", nodeUniqueId, stateName, id, highestPromised)

      val newData = id.number match {
        case newNumber if newNumber > highestPromised =>
          d.copy(progress = journalProgress(Progress.highestPromisedLens.set(p, id.number))) // TOOD lens
        case _ => d
      }

      journal.accept(a)
      send(sender, AcceptAck(id, nodeUniqueId, p))
      stay using newData
  }

  val ignoreHeartbeatStateFunction: StateFunction = {
    // ingore a HeartBeat which has not already been handled
    case Event(PaxosActor.HeartBeat, _) =>
      // we don't trace this as it would be noise
      stay
  }

  val ignoreNotTimedOutCheck: StateFunction = {
    case Event(PaxosActor.CheckTimeout, _) =>
      // we don't trace this as it would be noise
      stay
  }

  val commonStateFunction: StateFunction = retransmissionStateFunction orElse prepareStateFunction orElse acceptStateFunction orElse ignoreHeartbeatStateFunction orElse ignoreNotTimedOutCheck

  val followerStateFunction: StateFunction = {
    // commit message
    case e@Event(c@Commit(i, heartbeat), oldData) =>
      trace(stateName, e.stateData, sender, e.event)
      // if the leadership has changed or we see a new heartbeat from the same leader cancel any timeout work
      val newData = heartbeat match {
        case heartbeat if heartbeat > oldData.leaderHeartbeat || i.number > oldData.progress.highestPromised =>
          oldData.copy(leaderHeartbeat = heartbeat, prepareResponses = SortedMap.empty[Identifier, Map[Int, PrepareResponse]], timeout = freshTimeout(randomInterval)) // TODO lens
        case _ =>
          log.debug("Node {} {} not setting a new timeout from commit {}", nodeUniqueId, stateName, c)
          oldData
      }
      if (i.logIndex <= oldData.progress.highestCommitted.logIndex) {
        // no new commit information in this message
        stay using newData
      } else {
        // attempt a fast-forward commit up to the named slot
        val (newProgress, _) = commit(stateName, stateData, i, newData.progress)
        val newHighestCommitted = newProgress.highestCommitted.logIndex
        // if we did not commit up to the value in the commit message request retransmission of missing values
        if (newHighestCommitted < i.logIndex) {
          log.info("Node {} attempted commit of {} for log index {} found missing accept messages so have only committed up to {} and am requesting retransmission", nodeUniqueId, i, i.logIndex, newHighestCommitted)
          send(sender, RetransmitRequest(nodeUniqueId, i.from, newHighestCommitted))
        }
        stay using PaxosData.progressLens.set(newData, newProgress)
      }

    // upon timeout having not issued low prepares start the leader takeover protocol by issuing a min prepare
    case e@Event(PaxosActor.CheckTimeout, data@PaxosData(progress, _, to, _, prepareResponses, _, _, _)) if clock() >= to && prepareResponses.isEmpty =>
      trace(stateName, e.stateData, sender, e.event)
      stay using handleFollowerTimeout(nodeUniqueId, stateName, data)

    // on a timeout where we have issued a low prepare but not yet received a majority response we should rebroadcast the low prepare
    case e@Event(PaxosActor.CheckTimeout, data@PaxosData(_, _, to, _, prepareResponses, _, _, _)) if clock() >= to && prepareResponses.nonEmpty =>
      trace(stateName, e.stateData, sender, e.event)
      stay using handleResendLowPrepares(nodeUniqueId, stateName, data)

    // having issued a low prepare track responses and promote to recover only if we see insufficient evidence of a leader in the responses
    case e@Event(vote: PrepareResponse, data@PaxosData(_, _, _, _, prepareResponses, _, _, _)) if prepareResponses.nonEmpty =>
      trace(stateName, e.stateData, sender(), e.event)
      handLowPrepareResponse(nodeUniqueId, stateName, data, sender(), vote) match {
        case LowPrepareResponseResult(Recoverer, newData, highPrepares) =>
          // journal new progress with our self promise then broadcast the high prepares
          journalProgress(data.progress)
          highPrepares foreach {
            broadcast(_)
          }
          goto(Recoverer) using newData
        case LowPrepareResponseResult(_, newData, _) => stay using newData
        case x => throw new AssertionError(s"unexpected result $x")
      }

    // if we backdown to follower on a majority AcceptNack we may see a late accept response that we will ignore
    case e@Event(ar: AcceptResponse, data) =>
      trace(stateName, e.stateData, sender, e.event)
      log.debug("Node {} {} ignoring accept response {}", nodeUniqueId, stateName, ar)
      stay

    // we may see a prepare response that we are not awaiting any more which we will ignore
    case e@Event(pr: PrepareResponse, PaxosData(_, _, _, _, prepareResponses, _, _, _)) if prepareResponses.isEmpty =>
      trace(stateName, e.stateData, sender, e.event)
      log.debug("Node {} {} ignoring late PrepareResponse {}", nodeUniqueId, stateName, pr)
      stay
  }

  val notLeaderStateFunction: StateFunction = {
    case e@Event(v: CommandValue, data) =>
      trace(stateName, e.stateData, sender, e.event)
      val notLeader = NotLeader(nodeUniqueId, v.msgId)
      log.debug("Node {} responding with {}", nodeUniqueId, notLeader)
      send(sender, notLeader)
      stay
  }

  when(Follower)(followerStateFunction orElse notLeaderStateFunction orElse commonStateFunction)

  val returnToFollowerStateFunction: StateFunction = {
    /**
     * If we see a commit at a higher slot we should backdown and request retransmission.
     * If we see a commit for the same slot but with a higher number from a node with a higher node unique id we should backdown.
     */
    case e@Event(c@Commit(i@Identifier(from, number, logIndex), _), oldData@HighestCommittedIndexAndEpoch(committedLogIndex, epoch)) if logIndex > committedLogIndex || number > epoch && logIndex == committedLogIndex =>
      trace(stateName, e.stateData, sender, e.event)
      val newProgress = handleReturnToFollowerOnHigherCommit(c, oldData, stateName, sender)
      goto(Follower) using backdownData(PaxosData.progressLens.set(oldData, newProgress))

    case e@Event(Commit(id@Identifier(_, _, logIndex), _), data) =>
      trace(stateName, e.stateData, sender, e.event)
      log.debug("Node {} {} ignoring commit {} as have as high progress {}", nodeUniqueId, stateName, id, data.progress)
      stay
  }

  // FIXME add explicit test for this
  def backdownData(data: PaxosData) = PaxosActor.backdownData(data, randomTimeout)

  def requestRetransmissionIfBehind(data: PaxosData, sender: ActorRef, from: Int, highestCommitted: Identifier): Unit = {
    val highestCommittedIndex = data.progress.highestCommitted.logIndex
    val highestCommittedIndexOther = highestCommitted.logIndex
    if (highestCommittedIndexOther > highestCommittedIndex) {
      log.info("Node {} Recoverer requesting retransmission to target {} with highestCommittedIndex {}", nodeUniqueId, from, highestCommittedIndex)
      send(sender, RetransmitRequest(nodeUniqueId, from, highestCommittedIndex))
    }
  }

  val takeoverStateFunction: StateFunction = {

    case e@Event(vote: PrepareResponse, oldData: PaxosData) =>
      trace(stateName, e.stateData, sender, e.event)
      log.debug("Node {} Recoverer received a prepare response: {}", nodeUniqueId, vote)

      val (role, data) = handlePrepareResponse(nodeUniqueId, stateName, sender(), vote, oldData)
      goto(role) using data

  }

  val acceptResponseStateFunction: StateFunction = {
    // count accept response votes and commit
    case e@Event(vote: AcceptResponse, oldData) =>
      trace(stateName, e.stateData, sender, e.event)
      log.debug("Node {} {} {}", nodeUniqueId, stateName, vote)
      val (role, data) = handleAcceptResponse(nodeUniqueId, stateName, sender(), vote, oldData)
      goto(role) using data

  }

  /**
   * Here on a timeout we deal with either pending prepares or pending accepts putting a priorty on prepare handling
   * which backs down easily. Only if we have dealt with all timed out prepares do we handle timed out accepts which
   * is more aggressive as it attempts to go-higher than any other node number.
   */
  val resendStateFunction: StateFunction = {
    // if we have timed-out on prepare messages
    case e@Event(PaxosActor.CheckTimeout, data@PaxosData(_, _, timeout, _, prepareResponses, _, _, _)) if prepareResponses.nonEmpty && clock() > timeout =>
      trace(stateName, e.stateData, sender, e.event)
      // prepares we only retransmit as we handle all outcomes on the prepare response such as backing down
      log.debug("Node {} {} time-out on {} prepares", nodeUniqueId, stateName, prepareResponses.size)
      prepareResponses foreach {
        case (id, _) =>
          // broadcast is preferred as previous responses may be stale
          broadcast(Prepare(id))
      }
      stay using PaxosData.timeoutLens.set(data, freshTimeout(randomInterval))

    // else if we have timed-out on accept messages
    case e@Event(PaxosActor.CheckTimeout, data@PaxosData(_, _, timeout, _, _, _, accepts, _)) if accepts.nonEmpty && clock() >= timeout =>
      trace(stateName, e.stateData, sender, e.event)
      stay using handleResendAccepts(stateName, data, timeout)
  }

  when(Recoverer)(takeoverStateFunction orElse
    acceptResponseStateFunction orElse
    resendStateFunction orElse
    returnToFollowerStateFunction orElse
    notLeaderStateFunction orElse
    commonStateFunction)

  val leaderStateFunction: StateFunction = {

    // heartbeats the highest commit message
    case e@Event(PaxosActor.HeartBeat, data) =>
      trace(stateName, e.stateData, sender, e.event)
      val c = Commit(data.progress.highestCommitted)
      broadcast(c)
      stay

    // broadcasts a new client value
    case e@Event(value: CommandValue, data) =>
      trace(stateName, e.stateData, sender, e.event)
      log.debug("Node {} {} value {}", nodeUniqueId, stateName, value)

      data.epoch match {
        // the following 'if' check is an invariant of the algorithm we will throw and kill the actor if we have no match
        case Some(epoch) if data.progress.highestPromised <= epoch =>
          // compute next slot
          val lastLogIndex: Long = data.acceptResponses.lastOption match {
            case Some((id, _)) => id.logIndex
            case _ => data.progress.highestCommitted.logIndex
          }
          // create accept
          val nextLogIndex = lastLogIndex + 1
          val aid = Identifier(nodeUniqueId, data.epoch.get, nextLogIndex)
          val accept = Accept(aid, value)

          // self accept
          journal.accept(accept)
          // register self
          val updated = data.acceptResponses + (aid -> AcceptResponsesAndTimeout(randomTimeout, accept, Map(nodeUniqueId -> AcceptAck(aid, nodeUniqueId, data.progress))))
          // broadcast
          broadcast(accept)
          // add the sender our client map
          val clients = data.clientCommands + (accept.id ->(value, sender))
          stay using PaxosData.leaderLens.set(data, (SortedMap.empty, updated, clients))
        case x =>
          throw new AssertionError(s"Invariant violation as '$x' does not match case Some(epoch) if ${data.progress.highestPromised} <= epoch")
      }

    // ignore late vote as we would have transitioned on a majority ack
    case e@Event(vote: PrepareResponse, _) =>
      trace(stateName, e.stateData, sender, e.event)
      log.debug("Node {} {} ignoring {}", nodeUniqueId, stateName, vote)
      stay
  }

  when(Leader)(leaderStateFunction orElse
    acceptResponseStateFunction orElse
    resendStateFunction orElse
    returnToFollowerStateFunction orElse
    commonStateFunction)

  whenUnhandled {
    case e@Event(msg, data) =>
      handleUnhandled(nodeUniqueId, stateName, sender, e)
      stay
  }

  def highestAcceptedIndex: Long = journal.bounds.max

  def highestNumberProgressed(data: PaxosData): BallotNumber = Seq(data.epoch, Option(data.progress.highestPromised), Option(data.progress.highestCommitted.number)).flatten.max

  def randomInterval: Long = {
    config.leaderTimeoutMin + ((config.leaderTimeoutMax - config.leaderTimeoutMin) * random.nextDouble()).toLong
  }

  /**
   * Returns the next timeout put using a testable clock clock.
   */
  def freshTimeout(interval: Long): Long = {
    val t = clock() + interval
    t
  }

  def randomTimeout = freshTimeout(randomInterval)

  type Epoch = Option[BallotNumber]
  type PrepareSelfVotes = SortedMap[Identifier, Option[Map[Int, PrepareResponse]]]

  @elidable(elidable.FINE)
  def trace(state: PaxosRole, data: PaxosData, sender: ActorRef, msg: Any): Unit = {}

  @elidable(elidable.FINE)
  def trace(state: PaxosRole, data: PaxosData, payload: CommandValue): Unit = {}

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
      Array[Byte]()
  }

  /**
   * Notifies clients that it is no longer the leader by sending them an exception.
   */
  def sendNoLongerLeader(clientCommands: Map[Identifier, (CommandValue, ActorRef)]): Unit = clientCommands foreach {
    case (id, (cmd, client)) =>
      log.warning("Sending NoLongerLeader to client {} the outcome of the client cmd {} at slot {} is unknown.", client, cmd, id.logIndex)
      send(client, new NoLongerLeaderException(nodeUniqueId, cmd.msgId))
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
 * This class reschedules a random interval Paxos.CheckTimeout used to timeout on responses and an evenly spaced Paxos.HeartBeat which is used by a leader. 
 */
abstract class PaxosActorWithTimeout(config: Configuration, nodeUniqueId: Int, broadcast: ActorRef, journal: Journal)
  extends PaxosActor(config, nodeUniqueId, broadcast, journal) {

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  private[this] def scheduleCheckTimeout(interval: Long) = context.system.scheduler.scheduleOnce(Duration(interval, MILLISECONDS), self, PaxosActor.CheckTimeout)

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
    context.system.scheduler.schedule(Duration(5, MILLISECONDS), Duration(heartbeatInterval, MILLISECONDS), self, PaxosActor.HeartBeat)
  }
}

/**
 * Tracks the responses to an accept message and when we timeout on getting a majority response
 * @param timeout The point in time we timeout.
 * @param accept The accept that we are awaiting responses.
 * @param responses The known responses.
 */
case class AcceptResponsesAndTimeout(timeout: Long, accept: Accept, responses: Map[Int, AcceptResponse])

object PaxosActor {

  import Ordering._

  case object CheckTimeout

  case object HeartBeat

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

  // Log the nodeUniqueID, stateName, stateData, sender and message for tracing purposes
  case class TraceData(nodeUniqueId: Int, stateName: PaxosRole, statData: PaxosData, sender: Option[ActorRef], message: Any)

  type Tracer = TraceData => Unit

  val freshAcceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout] = SortedMap.empty

  val minJournalBounds = JournalBounds(Long.MinValue, Long.MinValue)

  def backdownData(data: PaxosData, timeout: Long) = PaxosData.backdownLens.set(data, (SortedMap.empty, SortedMap.empty, Map.empty, None, timeout))

  object HighestCommittedIndex {
    def unapply(data: PaxosData) = Some(data.progress.highestCommitted.logIndex)
  }

  object HighestCommittedIndexAndEpoch {
    def unapply(data: PaxosData) = data.epoch match {
      case Some(number) => Some(data.progress.highestCommitted.logIndex, number)
      case _ => Some(data.progress.highestCommitted.logIndex, Journal.minBookwork.highestPromised)
    }
  }

}

