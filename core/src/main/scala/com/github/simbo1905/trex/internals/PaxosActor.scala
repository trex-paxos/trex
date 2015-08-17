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
 * Paxos Actor Finite State Machine using immutable messages and immutable state.
 *
 * @param config Configuration such as timeout durations and the cluster size.
 * @param nodeUniqueId The unique identifier of this node. This *must* be unique in the cluster which is required as of the Paxos algorithm to work properly and be safe.
 * @param broadcastRef An ActorRef through which the current cluster can be messaged.
 * @param journal The durable journal required to store the state of the node in a stable manner between crashes.
 */
abstract class PaxosActor(config: Configuration, val nodeUniqueId: Int, broadcastRef: ActorRef, val journal: Journal) extends FSM[PaxosRole, PaxosData]
with RetransmitHandler
with ReturnToFollowerHandler
with UnhandledHandler
with CommitHandler {

  import Ordering._

  val minPrepare = Prepare(Identifier(nodeUniqueId, BallotNumber(Int.MinValue, Int.MinValue), Long.MinValue))

  // tests can override this
  protected def clock() = {
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
      handleRetransmitRequest(r, oldData) foreach { response =>
        log.info(s"Node $nodeUniqueId retransmission response to node {} at logIndex {} with {} committed and {} proposed entries", r.from, r.logIndex, response.committed.size, response.uncommitted.size)
        send(sender, response)
      }
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
          oldData.copy(leaderHeartbeat = heartbeat, prepareResponses = SortedMap.empty[Identifier, Option[Map[Int, PrepareResponse]]], timeout = freshTimeout(randomInterval)) // TODO lens
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
    case e@Event(PaxosActor.CheckTimeout, data@PaxosData(progress, _, to, _, _, _, prepareResponses, _)) if clock() >= to && prepareResponses.isEmpty =>
      trace(stateName, e.stateData, sender, e.event)
      log.info("Node {} {} timed-out progress: {}", nodeUniqueId, stateName, progress)
      send(broadcastRef, minPrepare)
      // nak our own prepare
      val prepareSelfVotes = SortedMap.empty[Identifier, Option[Map[Int, PrepareResponse]]] ++
        Map(minPrepare.id -> Some(Map(nodeUniqueId -> PrepareNack(minPrepare.id, nodeUniqueId, progress, highestAcceptedIndex, data.leaderHeartbeat))))

      stay using PaxosData.timeoutPrepareResponsesLens.set(data, (freshTimeout(randomInterval), prepareSelfVotes))

    // on a timeout where we have issued a low prepare but not yet received a majority response we should rebroadcast the low prepare
    // FIXME no test for this
    case e@Event(PaxosActor.CheckTimeout, data@PaxosData(_, _, to, _, prepareResponses, _, _, _)) if clock() >= to && prepareResponses.nonEmpty =>
      trace(stateName, e.stateData, sender, e.event)
      log.debug("Node {} {} timed-out having already issued a low. rebroadcasting", nodeUniqueId, stateName)
      // FIXME test case for this
      send(broadcastRef, minPrepare)
      stay using PaxosData.timeoutPrepareResponsesLens.set(data, (freshTimeout(randomInterval), prepareResponses))

    // having issued a low prepare track responses and promote to recover only if we see insufficient evidence of a leader in the responses
    case e@Event(vote: PrepareResponse, data@PaxosData(progress, _, heartbeat, _, prepareResponses, _, _, _)) if prepareResponses.nonEmpty =>
      trace(stateName, e.stateData, sender(), e.event)
      val selfHighestSlot = progress.highestCommitted.logIndex
      val otherHighestSlot = vote.progress.highestCommitted.logIndex
      if (otherHighestSlot > selfHighestSlot) {
        log.debug("Node {} node {} committed slot {} requesting retransmission", nodeUniqueId, vote.from, otherHighestSlot)
        send(sender(), RetransmitRequest(nodeUniqueId, vote.from, progress.highestCommitted.logIndex)) // TODO test for this
        stay using backdownData(data)
      } else {
        data.prepareResponses.get(vote.requestId) match {
          case Some(Some(map)) =>
            val votes = map + (vote.from -> vote)
            // do we have a majority response such that we could successfully failover?
            if (votes.size > data.clusterSize / 2) {

              val largerHeartbeats = votes.values flatMap {
                case PrepareNack(_, _, evidenceProgress, _, evidenceHeartbeat) if evidenceHeartbeat > heartbeat =>
                  Some(evidenceHeartbeat)
                case _ =>
                  None
              }

              lazy val largerHeartbeatCount = largerHeartbeats.size

              val failover = if (largerHeartbeats.isEmpty) {
                // all clear the last leader must be dead take over the leadership
                log.info("Node {} Follower no heartbeats executing takeover protocol.", nodeUniqueId)
                true
              } else if (largerHeartbeatCount + 1 > data.clusterSize / 2) {
                // no need to failover as there is sufficient evidence to deduce that there is a leader which can contact a working majority
                log.info("Node {} Follower sees {} fresh heartbeats *not* execute the leader takeover protocol.", nodeUniqueId, largerHeartbeatCount)
                false
              } else {
                // insufficient evidence. this would be due to a complex network partition. if we don't attempt a
                // leader fail-over the cluster may halt. if we do we risk a leader duel. a duel is the lesser evil as you
                // can solve it by stopping a node until you heal the network partition(s). in the future the leader
                // may heartbeat at commit noop values probably when we have implemented the strong read
                // optimisation which will also prevent a duel.
                log.info("Node {} Follower sees {} heartbeats executing takeover protocol.",
                  nodeUniqueId, largerHeartbeatCount)
                true
              }

              if (failover) {
                val highestNumber = Seq(data.progress.highestPromised, data.progress.highestCommitted.number).max
                val maxCommittedSlot = data.progress.highestCommitted.logIndex
                val maxAcceptedSlot = highestAcceptedIndex
                // create prepares for the known uncommitted slots else a refresh prepare for the next higher slot than committed
                val prepares = recoverPrepares(highestNumber, maxCommittedSlot, maxAcceptedSlot)
                // make a promise to self not to accept higher numbered messages and journal that
                val selfPromise = prepares.head.id.number
                // accept our own promise and load from the journal any values previous accepted in those slots
                val prepareSelfVotes: SortedMap[Identifier, Option[Map[Int, PrepareResponse]]] =
                  (prepares map { prepare =>
                    val selfVote = Some(Map(nodeUniqueId -> PrepareAck(prepare.id, nodeUniqueId, data.progress, highestAcceptedIndex, data.leaderHeartbeat, journal.accepted(prepare.id.logIndex))))
                    prepare.id -> selfVote
                  })(scala.collection.breakOut)

                // the new leader epoch is the promise it made to itself
                val epoch: Option[BallotNumber] = Some(selfPromise)
                // make a promise to self not to accept higher numbered messages and journal that
                journal.save(Progress.highestPromisedLens.set(data.progress, selfPromise))
                // broadcast the prepare messages
                prepares foreach {
                  send(broadcastRef, _)
                }
                log.info("Node {} Follower broadcast {} prepare messages with {} transitioning Recoverer max slot index {}.", nodeUniqueId, prepares.size, selfPromise, maxAcceptedSlot)
                goto(Recoverer) using PaxosData.highestPromisedTimeoutEpochPrepareResponsesAcceptResponseLens.set(data, (selfPromise, freshTimeout(randomInterval), epoch, prepareSelfVotes, SortedMap.empty))
              } else {
                // other nodes are showing a leader behind a partial network partition has a majority so we backdown.
                // we update the known heartbeat in case that leader dies causing a new scenario were only this node can form a majority.
                stay using data.copy(prepareResponses = SortedMap.empty, leaderHeartbeat = largerHeartbeats.max) // TODO lens
              }
            } else {
              // need to await to hear from a majority
              stay using data.copy(prepareResponses = TreeMap(Map(minPrepare.id -> Option(votes)).toArray: _*)) // TODO lens
            }
          case _ =>
            // FIXME no test for this
            log.debug("Node {} {} is no longer awaiting responses to {} so ignoring", nodeUniqueId, stateName, vote.requestId)
            stay using backdownData(data)
        }
      }

    // if we backdown to follower on a majority AcceptNack we may see a late accept response that we will ignore
    // FIXME no test for this
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
    // FIXME should also match on same logIndex but higher nodeUniqueNumber
    case e@Event(c@Commit(i@Identifier(from, _, logIndex), _), oldData: PaxosData) if logIndex > oldData.progress.highestCommitted.logIndex =>
      trace(stateName, e.stateData, sender, e.event)
      val newProgress = handleReturnToFollowerOnHigherCommit(c, oldData, stateName, sender)
      goto(Follower) using PaxosData.timeoutLens.set(PaxosData.progressLens.set(oldData, newProgress), randomTimeout)

    case e@Event(Commit(id@Identifier(_, _, logIndex), _), data) =>
      trace(stateName, e.stateData, sender, e.event)
      log.debug("Node {} {} ignoring commit {} as have as high progress {}", nodeUniqueId, stateName, id, data.progress)
      stay
  }

  def backdownData(data: PaxosData) = PaxosData.backdownLens.set(data, (SortedMap.empty, SortedMap.empty, Map.empty, None, freshTimeout(randomInterval)))

  def requestRetransmissionIfBehind(data: PaxosData, sender: ActorRef, from: Int, highestCommitted: Identifier): Unit = {
    val highestCommittedIndex = data.progress.highestCommitted.logIndex
    val highestCommittedIndexOther = highestCommitted.logIndex
    if (highestCommittedIndexOther > highestCommittedIndex) {
      log.info("Node {} Recoverer requesting retransmission to target {} with highestCommittedIndex {}", nodeUniqueId, from, highestCommittedIndex)
      send(sender, RetransmitRequest(nodeUniqueId, from, highestCommittedIndex))
    }
  }

  val takeoverStateFunction: StateFunction = {

    case e@Event(vote: PrepareResponse, data: PaxosData) =>
      trace(stateName, e.stateData, sender, e.event)
      log.debug("Node {} Recoverer received a prepare response: {}", nodeUniqueId, vote)

      requestRetransmissionIfBehind(data, sender(), vote.from, vote.progress.highestCommitted)

      val id = vote.requestId

      if (id.from != nodeUniqueId) {
        log.info("Node {} {} message with id {} is not for this node", nodeUniqueId, stateName, id)
        stay // FIXME test for this and do we want to add such a guard to the accept response processor?
      } else {
        data.prepareResponses.getOrElse(id, None) match {
          case None =>
            // we already had a majority positive response so nothing to do
            log.debug("Node {} Ignored prepare response as no longer tracking this request: {}", nodeUniqueId, vote)
            stay
          case Some(map) =>
            // register the vote
            val votes = map + (vote.from -> vote)

            // if we have a majority response which show more slots to recover issue new prepare messages
            val dataWithExpandedPrepareResponses = if (votes.size > data.clusterSize / 2) {
              // issue more prepares there are more accepted slots than we so far ran recovery upon
              val (Identifier(_, _, highestAcceptedIndex), _) = data.prepareResponses.last
              val highestAcceptedIndexOther = votes.values.map(_.highestAcceptedIndex).max
              if (highestAcceptedIndexOther > highestAcceptedIndex) {
                val prepares = (highestAcceptedIndex + 1) to highestAcceptedIndexOther map { id =>
                  Prepare(Identifier(nodeUniqueId, data.epoch.get, id))
                }
                log.info("Node {} Recoverer broadcasting {} new prepare messages for expanded slots {} to {}", nodeUniqueId, prepares.size, (highestAcceptedIndex + 1), highestAcceptedIndexOther)
                prepares foreach { p =>
                  log.debug("Node {} sending {}", nodeUniqueId, p)
                  send(broadcastRef, p)
                }

                // accept our own prepare if we have not made a higher promise
                val newPrepareSelfVotes: SortedMap[Identifier, Option[Map[Int, PrepareResponse]]] =
                  (prepares map { prepare =>
                    val ackOrNack = if (prepare.id.number >= data.progress.highestPromised) {
                      PrepareAck(prepare.id, nodeUniqueId, data.progress, highestAcceptedIndex, data.leaderHeartbeat, journal.accepted(prepare.id.logIndex))
                    } else {
                      // FIXME no test for this
                      PrepareNack(prepare.id, nodeUniqueId, data.progress, highestAcceptedIndex, data.leaderHeartbeat)
                    }
                    val selfVote = Some(Map(nodeUniqueId -> ackOrNack))
                    (prepare.id -> selfVote)
                  })(scala.collection.breakOut)
                // FIXME no test
                PaxosData.prepareResponsesLens.set(data, data.prepareResponses ++ newPrepareSelfVotes)
              } else {
                data
              }
            } else {
              data
            }
            // tally the votes
            val (positives, negatives) = votes.partition {
              case (_, response) => response.isInstanceOf[PrepareAck]
            }
            if (positives.size > data.clusterSize / 2) {
              // success gather any values
              val accepts = positives.values.map(_.asInstanceOf[PrepareAck]).flatMap(_.highestUncommitted)
              val accept = if (accepts.isEmpty) {
                val accept = Accept(id, NoOperationCommandValue)
                log.info("Node {} {} got a majority of positive prepare response with no value sending fresh NO_OPERATION accept message {}", nodeUniqueId, stateName, accept)
                accept
              } else {
                val max = accepts.maxBy(_.id.number)
                val accept = Accept(id, max.value)
                log.info("Node {} {} got a majority of positive prepare response with highest accept message {} sending fresh message {}", nodeUniqueId, stateName, max.id, accept)
                accept
              }
              // broadcast accept
              log.debug("Node {} {} sending {}", nodeUniqueId, stateName, accept)
              send(broadcastRef, accept)
              // only accept your own broadcast if we have not made a higher promise whilst awaiting responses from other nodes
              val selfResponse: AcceptResponse = if (accept.id.number >= dataWithExpandedPrepareResponses.progress.highestPromised) {
                // FIXME had the inequality wrong way around and Recoverer tests didn't catch it. Add a test to cover this.
                log.debug("Node {} {} accepting own message {}", nodeUniqueId, stateName, accept.id)
                journal.accept(accept)
                AcceptAck(accept.id, nodeUniqueId, dataWithExpandedPrepareResponses.progress)
              } else {
                // FIXME no test
                log.debug("Node {} {} not accepting own message with number {} as have made a higher promise {}", nodeUniqueId, stateName, accept.id.number, dataWithExpandedPrepareResponses.progress.highestPromised)
                AcceptNack(accept.id, nodeUniqueId, dataWithExpandedPrepareResponses.progress)
              }
              // create a fresh vote for your new accept message
              val selfVoted = dataWithExpandedPrepareResponses.acceptResponses + (accept.id -> Some(Map(nodeUniqueId -> selfResponse)))
              // we are no longer awaiting responses to the prepare
              val expandedRecover = dataWithExpandedPrepareResponses.prepareResponses
              val updatedPrepares = expandedRecover - vote.requestId
              if (updatedPrepares.isEmpty) {
                // we have completed recovery of the values in the slots so we now switch to stable Leader state
                val newData = PaxosData.leaderLens.set(dataWithExpandedPrepareResponses, (SortedMap.empty, selfVoted, Map.empty))
                log.info("Node {} {} has issued accept messages for all prepare messages to promoting to be Leader.", nodeUniqueId, stateName)
                goto(Leader) using newData.copy(clientCommands = Map.empty) // TODO lens?
              } else {
                log.info("Node {} {} is still recovering {} slots", nodeUniqueId, stateName, updatedPrepares.size)
                stay using PaxosData.leaderLens.set(dataWithExpandedPrepareResponses, (updatedPrepares, selfVoted, Map.empty))
              }
            } else if (negatives.size > data.clusterSize / 2) {
              log.info("Node {} {} received {} prepare nacks returning to follower", nodeUniqueId, stateName, negatives.size)
              // FIXME this wasn't setting a new timeout and not failing any tests was that a potential untested lockup
              goto(Follower) using backdownData(data)
            }
            else {
              val updated = data.prepareResponses + (vote.requestId -> Some(votes))
              stay using PaxosData.prepareResponsesLens.set(dataWithExpandedPrepareResponses, updated)
            }
        }
      }
  }

  val acceptResponseStateFunction: StateFunction = {
    // count accept response votes and commit
    case e@Event(vote: AcceptResponse, oldData) =>
      trace(stateName, e.stateData, sender, e.event)
      log.debug("Node {} {} {}", nodeUniqueId, stateName, vote)
      oldData.acceptResponses.get(vote.requestId) match {
        case Some(votesOption) =>
          val latestVotes = votesOption.get + (vote.from -> vote)
          if (latestVotes.size > oldData.clusterSize / 2) {
            // requestRetransmission if behind FIXME what if it cannot get a majority response will it get stuck not asking for retransmission?
            val highestCommittedIndex = oldData.progress.highestCommitted.logIndex
            val (target, highestCommittedIndexOther) = latestVotes.values.map(ar => ar.from -> ar.progress.highestCommitted.logIndex).maxBy(_._2)
            if (highestCommittedIndexOther > highestCommittedIndex) {
              log.debug("Node {} {} requesting retransmission to target {} with highestCommittedIndex {}", nodeUniqueId, stateName, target, highestCommittedIndex)
              send(sender, RetransmitRequest(nodeUniqueId, target, highestCommittedIndex))
            }
          }
          val (positives, negatives) = latestVotes.toList.partition(_._2.isInstanceOf[AcceptAck])
          if (negatives.size > oldData.clusterSize / 2) {
            log.info("Node {} {} received a majority accept nack so has lost leadership becoming a follower.", nodeUniqueId, stateName)
            sendNoLongerLeader(oldData.clientCommands)
            goto(Follower) using backdownData(oldData)
          } else if (positives.size > oldData.clusterSize / 2) {
            // this slot is fixed record that we are not awaiting any more votes 
            val updated = oldData.acceptResponses + (vote.requestId -> None)

            // grab all the accepted values from the beginning of the tree map
            val (committable, uncommittable) = updated.span { case (_, rs) => rs.isEmpty }
            log.debug("Node " + nodeUniqueId + " {} vote {} committable {} uncommittable {}", stateName, vote, committable, uncommittable)

            // this will have dropped the committable 
            val votesData = PaxosData.acceptResponsesLens.set(oldData, uncommittable)

            // attempt an in-sequence commit
            if (committable.isEmpty) {
              stay using votesData // gap in committable sequence
            } else if (committable.head._1.logIndex != votesData.progress.highestCommitted.logIndex + 1) {
              log.error(s"Node $nodeUniqueId $stateName invariant violation: $stateName has committable work which is not contiguous with progress implying we have not issued Prepare/Accept messages for the correct range of slots. Returning to follower.")
              sendNoLongerLeader(oldData.clientCommands)
              goto(Follower) using backdownData(oldData)
            } else {
              val (newProgress, results) = commit(stateName, stateData, committable.last._1, votesData.progress)

              // FIXME test that the send of the commit happens after saving the progress
              // FIXME was no test checking that this was broadcast not just replied to sender
              send(broadcastRef, Commit(newProgress.highestCommitted))

              if (stateName == Leader && oldData.clientCommands.nonEmpty) {
                // TODO the nonEmpty guard is due to test data not setting this should fix the test data and remove it

                val (committedIds, _) = results.unzip

                val (responds, remainders) = oldData.clientCommands.partition {
                  idCmdRef: (Identifier, (CommandValue, ActorRef)) =>
                    val (id, (_, _)) = idCmdRef
                    committedIds.contains(id)
                }

                log.debug("Node {} {} post commit has responds.size={}, remainders.size={}", nodeUniqueId, stateName, responds.size, remainders.size)
                results foreach { case (id, bytes) =>
                  responds.get(id) foreach { case (cmd, client) =>
                      log.debug("sending response from accept {} to {}", id, client)
                      send(client, bytes)
                  }
                }
                stay using PaxosData.progressLens.set(votesData, newProgress).copy(clientCommands = remainders) // TODO new lens?
              } else {
                stay using PaxosData.progressLens.set(votesData, newProgress)
              }
            }
          } else {
            // insufficient votes keep counting
            val updated = oldData.acceptResponses + (vote.requestId -> Some(latestVotes))
            log.debug("Node {} {} insufficent votes for {} have {}", nodeUniqueId, stateName, vote.requestId, updated)
            stay using PaxosData.acceptResponsesLens.set(oldData, updated)
          }
        case None =>
          log.debug("Node {} {} ignoring late response as saw a majority response: {}", nodeUniqueId, stateName, vote)
          stay
      }
  }

  val resendPreparesStateFunction: StateFunction = {
    case e@Event(PaxosActor.CheckTimeout, data@PaxosData(_, _, timeout, _, prepareResponses, _, _, _)) if prepareResponses.nonEmpty && clock() > timeout =>
      trace(stateName, e.stateData, sender, e.event)
      // prepares we only retransmit as we handle all outcomes on the prepare response such as backing down
      log.debug("Node {} {} time-out on {} prepares", nodeUniqueId, stateName, prepareResponses.size)
      prepareResponses foreach {
        case (id, None) => // is committed
        // FIXME no test
        case (id, _) =>
          // broadcast is preferred as previous responses may be stale
          send(broadcastRef, Prepare(id))
      }
      stay using PaxosData.timeoutLens.set(data, freshTimeout(randomInterval))
  }

  val resendAcceptsStateFunction: StateFunction = {

    // FIXME set the fresh timeout when send accepts but given leader sends accepts for clients we need to timeout on individual accepts
    case e@Event(PaxosActor.CheckTimeout, data@PaxosData(_, _, timeout, _, _, _, accepts, _)) if accepts.nonEmpty && clock() > timeout =>
      trace(stateName, e.stateData, sender, e.event)

      // accepts didn't get a majority yes/no and saw no higher commits so we increment the ballot number and broadcast
      val newData = data.acceptResponses match {
        case acceptResponses if acceptResponses.nonEmpty =>
          // the slots which have not yet committed
          val indexes = acceptResponses.keys.map(_.logIndex)
          // highest promised or committed at this node
          val highestLocal: BallotNumber = highestNumberProgressed(data)
          // numbers in any responses including our own possibly stale self response
          val proposalNumbers = (acceptResponses.values flatMap {
            _ map {
              _.values.flatMap { r =>
                Set(r.progress.highestCommitted.number, r.progress.highestPromised)
              }
            }
          }).flatten // TODO for-comprehension?
        // the max known
        val maxNumber = (proposalNumbers.toSeq :+ highestLocal).max
          // check whether we were actively rejected
          if (maxNumber > data.epoch.get) {
            val higherNumber = maxNumber.copy(counter = maxNumber.counter + 1, nodeIdentifier = nodeUniqueId) // TODO lens?
            log.info(s"Node $nodeUniqueId {} time-out on accept old epoch {} new epoch {}", stateName, maxNumber, data.epoch.get, higherNumber)

            val freshAccepts = indexes map { slot =>
              val oldAccept = journal.accepted(slot)

              // TODO: We're making a naked get on the Option here. What if it is None? Need better handling.
              Accept(Identifier(nodeUniqueId, higherNumber, slot), oldAccept.get.value)
            }
            log.info("Node {} {} time-out on {} accepts", nodeUniqueId, stateName, freshAccepts.size)
            freshAccepts foreach { a =>
              journal.accept(a)
              send(broadcastRef, a)
            }
            PaxosData.acceptResponsesEpochTimeoutLens.set(data, (SortedMap.empty, Some(higherNumber), data.timeout))
          } else {
            log.info("Node {} {} time-out on accepts same epoch {} resending {}", nodeUniqueId, stateName, data.epoch.get, indexes)
            indexes foreach {
              journal.accepted(_) foreach {
                send(broadcastRef, _)
              }
            }
            data
          }
        case _ =>
          data
      }

      stay using PaxosData.timeoutLens.set(newData, freshTimeout(randomInterval))
  }

  when(Recoverer)(takeoverStateFunction orElse
    acceptResponseStateFunction orElse
    resendPreparesStateFunction orElse
    resendAcceptsStateFunction orElse
    returnToFollowerStateFunction orElse
    notLeaderStateFunction orElse
    commonStateFunction)

  val leaderStateFunction: StateFunction = {

    // heatbeats the highest commit message
    case e@Event(PaxosActor.HeartBeat, data) =>
      trace(stateName, e.stateData, sender, e.event)
      val c = Commit(data.progress.highestCommitted)
      send(broadcastRef, c)
      stay

    // broadcasts a new client value
    case e@Event(value: CommandValue, data) =>
      trace(stateName, e.stateData, sender, e.event)
      log.debug("Node {} {} value {}", nodeUniqueId, stateName, value)

      data.epoch match {
        // the following 'if' check is an invariant of the algorithm we will throw and kill the actor if we have no match
        case Some(epoch) if data.progress.highestPromised <= epoch =>
          // compute next slot
          val lastLogIndex: Long = if (data.acceptResponses.isEmpty) {
            data.progress.highestCommitted.logIndex
          } else {
            data.acceptResponses.last._1.logIndex
          }
          // create accept
          val nextLogIndex = lastLogIndex + 1
          val aid = Identifier(nodeUniqueId, data.epoch.get, nextLogIndex)
          val accept = Accept(aid, value)

          // self accept
          journal.accept(accept)
          // register self
          val updated = data.acceptResponses + (aid -> Some(Map(nodeUniqueId -> AcceptAck(aid, nodeUniqueId, data.progress))))
          // broadcast
          send(broadcastRef, accept)
          // add the sender our client map
          val clients = data.clientCommands + (accept.id ->(value, sender))
          stay using PaxosData.leaderLens.set(data, (SortedMap.empty, updated, clients))
      }

    // ignore late vote as we would have transitioned on a majority ack
    case e@Event(vote: PrepareResponse, _) =>
      trace(stateName, e.stateData, sender, e.event)
      log.debug("Node {} {} ignoring {}", nodeUniqueId, stateName, vote)
      stay
  }

  when(Leader)(leaderStateFunction orElse
    acceptResponseStateFunction orElse
    resendPreparesStateFunction orElse
    resendAcceptsStateFunction orElse
    returnToFollowerStateFunction orElse
    commonStateFunction)

  whenUnhandled {
    case e@Event(msg, data) =>
      handleUnhandled(nodeUniqueId, stateName, sender, e)
      stay
  }

  def highestAcceptedIndex = journal.bounds.max

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

  /**
   * Generates fresh prepare messages targeting the range of slots from the highest committed to one higher than the highest accepted slot positions.
   * @param highest Highest number known to this node.
   * @param highestCommittedIndex Highest slot committed at this node.
   * @param highestAcceptedIndex Highest slot where a value has been accepted by this node.
   */
  def recoverPrepares(highest: BallotNumber, highestCommittedIndex: Long, highestAcceptedIndex: Long) = {
    val BallotNumber(counter, _) = highest
    val higherNumber = BallotNumber(counter + 1, nodeUniqueId)
    val prepares = (highestCommittedIndex + 1) to (highestAcceptedIndex + 1) map {
      slot => Prepare(Identifier(nodeUniqueId, higherNumber, slot))
    }
    if (prepares.nonEmpty) prepares else Seq(Prepare(Identifier(nodeUniqueId, higherNumber, highestCommittedIndex + 1))) // FIXME empty was not picked up in unit test only when first booting a cluster
  }

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
 * We use a stateless Actor FSM pattern. This immutable case class holds the state of a node in the cluster.
 * Note that for testing this class does not schedule and manage its own timeouts. Use the subclass which
 * schedules its timeout rather than this baseclass.
 *
 * @param progress The highest promised and highest committed progress of a node in the cluster.
 * @param leaderHeartbeat The last heartbeat value seen from a leader. Note that clocks are not synced so this value is only used as evidence that a stable leader is up whereas the paxos number and committed slot are taken as authoritative that a new leader is making progress.
 * @param timeout The next randomised point in time that this node will timeout. Followers timeout on Commit messages and become a Recoverer. Recoverers timeout on PrepareResponses and AcceptResponses. Leaders timeout on AcceptResponses.
 * @param clusterSize The current size of the cluster.
 * @param prepareResponses The work outstanding uncommitted proposed work of the leader take over phase during the recovery of a leader failover. Each key is an identifier of a prepare for which we are collecting a majority response to determine the highest proposed value of the previous leader if any.
 * @param epoch The leaders paxos number when leading.
 * @param acceptResponses Tracking of responses to accept messages when Recoverer or Leader. Each key is an identifier of the command we want to commit. Each value is either the votes of each cluster node else None if the slot is ready to commit.
 * @param clientCommands The client work outstanding with the leader. The map key is the accept identifier and the value is a tuple of the client command and the client ref.
 */
case class PaxosData(progress: Progress,
                     leaderHeartbeat: Long,
                     timeout: Long,
                     clusterSize: Int,
                     prepareResponses: SortedMap[Identifier, Option[Map[Int, PrepareResponse]]] = SortedMap.empty[Identifier, Option[Map[Int, PrepareResponse]]](Ordering.IdentifierLogOrdering),
                     epoch: Option[BallotNumber] = None,
                     acceptResponses: SortedMap[Identifier, Option[Map[Int, AcceptResponse]]] = SortedMap.empty[Identifier, Option[Map[Int, AcceptResponse]]](Ordering.IdentifierLogOrdering),
                     clientCommands: Map[Identifier, (CommandValue, ActorRef)] = Map.empty)

object PaxosData {
  val prepareResponsesLens = Lens(
    get = (_: PaxosData).prepareResponses,
    set = (nodeData: PaxosData, prepareResponses: SortedMap[Identifier, Option[Map[Int, PrepareResponse]]]) => nodeData.copy(prepareResponses = prepareResponses))

  val acceptResponsesLens = Lens(
    get = (_: PaxosData).acceptResponses,
    set = (nodeData: PaxosData, acceptResponses: SortedMap[Identifier, Option[Map[Int, AcceptResponse]]]) => nodeData.copy(acceptResponses = acceptResponses)
  )

  val clientCommandsLens = Lens(
    get = (_: PaxosData).clientCommands,
    set = (nodeData: PaxosData, clientCommands: Map[Identifier, (CommandValue, ActorRef)]) => nodeData.copy(clientCommands = clientCommands)
  )

  val acceptResponsesClientCommandsLens = Lens(
    get = (n: PaxosData) => ((acceptResponsesLens(n), clientCommandsLens(n))),
    set = (n: PaxosData, value: (SortedMap[Identifier, Option[Map[Int, AcceptResponse]]], Map[Identifier, (CommandValue, ActorRef)])) =>
      value match {
        case (acceptResponses: SortedMap[Identifier, Option[Map[Int, AcceptResponse]]], clientCommands: Map[Identifier, (CommandValue, ActorRef)]) =>
          acceptResponsesLens.set(clientCommandsLens.set(n, clientCommands), acceptResponses)
      }
  )

  val timeoutLens = Lens(
    get = (_: PaxosData).timeout,
    set = (nodeData: PaxosData, timeout: Long) => nodeData.copy(timeout = timeout)
  )

  val epochLens = Lens(
    get = (_: PaxosData).epoch,
    set = (nodeData: PaxosData, epoch: Option[BallotNumber]) => nodeData.copy(epoch = epoch)
  )

  val leaderLens = Lens(
    get = (nodeData: PaxosData) => ((prepareResponsesLens(nodeData), acceptResponsesLens(nodeData), clientCommandsLens(nodeData))),
    set = (nodeData: PaxosData, value: (SortedMap[Identifier, Option[Map[Int, PrepareResponse]]],
      SortedMap[Identifier, Option[Map[Int, AcceptResponse]]],
      Map[Identifier, (CommandValue, ActorRef)])) =>
      value match {
        case (prepareResponses: SortedMap[Identifier, Option[Map[Int, PrepareResponse]]],
        acceptResponses: SortedMap[Identifier, Option[Map[Int, AcceptResponse]]],
        clientCommands: Map[Identifier, (CommandValue, ActorRef)]) =>
          prepareResponsesLens.set(acceptResponsesLens.set(clientCommandsLens.set(nodeData, clientCommands), acceptResponses), prepareResponses)
      }
  )

  val backdownLens = Lens(
    get = (n: PaxosData) => ((prepareResponsesLens(n), acceptResponsesLens(n), clientCommandsLens(n), epochLens(n), timeoutLens(n))),
    set = (n: PaxosData, value: (SortedMap[Identifier, Option[Map[Int, PrepareResponse]]], SortedMap[Identifier, Option[Map[Int, AcceptResponse]]], Map[Identifier, (CommandValue, ActorRef)], Option[BallotNumber], Long)) =>
      value match {
        case
          (prepareResponses: SortedMap[Identifier, Option[Map[Int, PrepareResponse]]],
          acceptResponses: SortedMap[Identifier, Option[Map[Int, AcceptResponse]]],
          clientCommands: Map[Identifier, (CommandValue, ActorRef)],
          epoch: Option[BallotNumber],
          timeout: Long
            ) => prepareResponsesLens.set(acceptResponsesLens.set(clientCommandsLens.set(epochLens.set(timeoutLens.set(n, timeout), epoch), clientCommands), acceptResponses), prepareResponses)
      }
  )

  val progressLens = Lens(
    get = (_: PaxosData).progress,
    set = (nodeData: PaxosData, progress: Progress) => nodeData.copy(progress = progress)
  )

  val highestPromisedLens = progressLens andThen Lens(get = (_: Progress).highestPromised, set = (progress: Progress, promise: BallotNumber) => progress.copy(highestPromised = promise))

  val timeoutPrepareResponsesLens = Lens(
    get = (nodeData: PaxosData) => ((timeoutLens(nodeData), prepareResponsesLens(nodeData))),
    set = (nodeData: PaxosData, value: (Long, SortedMap[Identifier, Option[Map[Int, PrepareResponse]]])) =>
      value match {
        case (timeout: Long, prepareResponses: SortedMap[Identifier, Option[Map[Int, PrepareResponse]]]) =>
          timeoutLens.set(prepareResponsesLens.set(nodeData, prepareResponses: SortedMap[Identifier, Option[Map[Int, PrepareResponse]]]), timeout)
      }
  )

  val acceptResponsesEpochTimeoutLens = Lens(
    get = (nodeData: PaxosData) => ((acceptResponsesLens(nodeData), epochLens(nodeData), timeoutLens(nodeData))),
    set = (nodeData: PaxosData, value: (SortedMap[Identifier, Option[Map[Int, AcceptResponse]]], Option[BallotNumber], Long)) =>
      value match {
        case (acceptResponses: SortedMap[Identifier, Option[Map[Int, AcceptResponse]]], epoch: Option[BallotNumber], timeout: Long) =>
          acceptResponsesLens.set(timeoutLens.set(epochLens.set(nodeData, epoch), timeout), acceptResponses)
      }
  )

  val highestPromisedTimeoutEpochPrepareResponsesAcceptResponseLens = Lens(
    get = (n: PaxosData) => (highestPromisedLens(n), timeoutLens(n), epochLens(n), prepareResponsesLens(n), acceptResponsesLens(n)),
    set = (n: PaxosData, value: (BallotNumber, Long, Option[BallotNumber], SortedMap[Identifier, Option[Map[Int, PrepareResponse]]], SortedMap[Identifier, Option[Map[Int, AcceptResponse]]])) =>
      value match {
        case (promise: BallotNumber, timeout: Long, epoch: Option[BallotNumber], prepareResponses: SortedMap[Identifier, Option[Map[Int, PrepareResponse]]], acceptResponses: SortedMap[Identifier, Option[Map[Int, AcceptResponse]]]) =>
          highestPromisedLens.set(timeoutLens.set(epochLens.set(prepareResponsesLens.set(acceptResponsesLens.set(n, acceptResponses), prepareResponses), epoch), timeout), promise)
      }
  )
}

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

  val freshAcceptResponses: SortedMap[Identifier, Option[Map[Int, AcceptResponse]]] = SortedMap.empty

  val minJournalBounds = JournalBounds(Long.MinValue, Long.MinValue)

  object HighestCommittedIndex {
    def unapply(data: PaxosData) = Some(data.progress.highestCommitted.logIndex)
  }

}

