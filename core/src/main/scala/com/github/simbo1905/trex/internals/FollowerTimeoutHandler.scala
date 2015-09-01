package com.github.simbo1905.trex.internals

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import com.github.simbo1905.trex.Journal
import scala.collection.SortedMap

import Ordering._

import scala.collection.immutable.TreeMap

trait FollowerTimeoutHandler {
  def log: LoggingAdapter

  def broadcast(msg: Any): Unit

  def minPrepare: Prepare

  def randomTimeout: Long

  def highestAcceptedIndex: Long

  def send(actor: ActorRef, msg: Any): Unit

  def backdownData(data: PaxosData): PaxosData

  def journal: Journal

  def computeFailover(log: LoggingAdapter, nodeUniqueId: Int, data: PaxosData, votes: Map[Int, PrepareResponse]): FailoverResult = FollowerTimeoutHandler.computeFailover(log, nodeUniqueId, data, votes)

  def recoverPrepares(nodeUniqueId: Int, highest: BallotNumber, highestCommittedIndex: Long, highestAcceptedIndex: Long) = FollowerTimeoutHandler.recoverPrepares(nodeUniqueId, highest, highestCommittedIndex, highestAcceptedIndex)

  def handleResendLowPrepares(nodeUniqueId: Int, stateName: PaxosRole, data: PaxosData): PaxosData = {
    log.debug("Node {} {} timed-out having already issued a low. rebroadcasting", nodeUniqueId, stateName)
    broadcast(minPrepare)
    PaxosData.timeoutLens.set(data, randomTimeout)
  }

  def handleFollowerTimeout(nodeUniqueId: Int, stateName: PaxosRole, data: PaxosData): PaxosData = {
    log.info("Node {} {} timed-out progress: {}", nodeUniqueId, stateName, data.progress)
    broadcast(minPrepare)
    // nack our own prepare
    val prepareSelfVotes = SortedMap.empty[Identifier, Option[Map[Int, PrepareResponse]]] ++
      Map(minPrepare.id -> Some(Map(nodeUniqueId -> PrepareNack(minPrepare.id, nodeUniqueId, data.progress, highestAcceptedIndex, data.leaderHeartbeat))))

    PaxosData.timeoutPrepareResponsesLens.set(data, (randomTimeout, prepareSelfVotes))
  }

  def handLowPrepareResponse(nodeUniqueId: Int, stateName: PaxosRole, data: PaxosData, sender: ActorRef, vote: PrepareResponse): LowPrepareResponseResult = {
    val selfHighestSlot = data.progress.highestCommitted.logIndex
    val otherHighestSlot = vote.progress.highestCommitted.logIndex
    if (otherHighestSlot > selfHighestSlot) {
      log.debug("Node {} node {} committed slot {} requesting retransmission", nodeUniqueId, vote.from, otherHighestSlot)
      send(sender, RetransmitRequest(nodeUniqueId, vote.from, data.progress.highestCommitted.logIndex))
      LowPrepareResponseResult(Follower, backdownData(data))
    } else {
      data.prepareResponses.get(vote.requestId) match {
        case Some(Some(map)) =>
          val votes = map + (vote.from -> vote)

          def haveMajorityResponse = votes.size > data.clusterSize / 2

          if (haveMajorityResponse) {

            computeFailover(log, nodeUniqueId, data, votes) match {
              case FailoverResult(failover, _) if failover =>
                val highestNumber = Seq(data.progress.highestPromised, data.progress.highestCommitted.number).max
                val maxCommittedSlot = data.progress.highestCommitted.logIndex
                // TODO issue #13 here we should look at the max of all votes
                val maxAcceptedSlot = highestAcceptedIndex
                // create prepares for the known uncommitted slots else a refresh prepare for the next higher slot than committed
                val prepares = recoverPrepares(nodeUniqueId, highestNumber, maxCommittedSlot, maxAcceptedSlot)
                // make a promise to self not to accept higher numbered messages and journal that
                prepares.headOption match {
                  case Some(p) =>
                    val selfPromise = p.id.number
                    // accept our own promise and load from the journal any values previous accepted in those slots
                    val prepareSelfVotes: SortedMap[Identifier, Option[Map[Int, PrepareResponse]]] =
                      (prepares map { prepare =>
                        val selfVote = Some(Map(nodeUniqueId -> PrepareAck(prepare.id, nodeUniqueId, data.progress, highestAcceptedIndex, data.leaderHeartbeat, journal.accepted(prepare.id.logIndex))))
                        prepare.id -> selfVote
                      })(scala.collection.breakOut)

                    // the new leader epoch is the promise it made to itself
                    val epoch: Option[BallotNumber] = Some(selfPromise)
                    log.info("Node {} Follower broadcast {} prepare messages with {} transitioning Recoverer max slot index {}.", nodeUniqueId, prepares.size, selfPromise, maxAcceptedSlot)
                    // make a promise to self not to accept higher numbered messages and journal that
                    LowPrepareResponseResult(Recoverer, PaxosData.highestPromisedTimeoutEpochPrepareResponsesAcceptResponseLens.set(data, (selfPromise, randomTimeout, epoch, prepareSelfVotes, SortedMap.empty)), prepares)
                  case None =>
                    log.error("this code should be unreachable")
                    LowPrepareResponseResult(Follower, data)
                }
              case FailoverResult(_, maxHeartbeat) =>
                // other nodes are showing a leader behind a partial network partition so we backdown.
                // we update the local known heartbeat in case that leader dies causing a new scenario were only this node can form a majority.
                LowPrepareResponseResult(Follower, data.copy(prepareResponses = SortedMap.empty, leaderHeartbeat = maxHeartbeat)) // TODO lens
            }
          } else {
            // need to wait until we hear from a majority
            LowPrepareResponseResult(Follower, data.copy(prepareResponses = TreeMap(Map(minPrepare.id -> Option(votes)).toArray: _*))) // TODO lens
          }
        case x =>
          log.debug("Node {} {} is no longer awaiting responses to {} so ignoring {}", nodeUniqueId, stateName, vote.requestId, x)
          LowPrepareResponseResult(Follower, data)
      }
    }
  }
}

case class FailoverResult(failover: Boolean, maxHeartbeat: Long)

case class LowPrepareResponseResult(role: PaxosRole, data: PaxosData, highPrepares: Seq[Prepare] = Seq.empty)

object FollowerTimeoutHandler {
  /**
   * Generates fresh prepare messages targeting the range of slots from the highest committed to one higher than the highest accepted slot positions.
   * @param highest Highest number known to this node.
   * @param highestCommittedIndex Highest slot committed at this node.
   * @param highestAcceptedIndex Highest slot where a value has been accepted by this node.
   */
  def recoverPrepares(nodeUniqueId: Int, highest: BallotNumber, highestCommittedIndex: Long, highestAcceptedIndex: Long) = {
    val BallotNumber(counter, _) = highest
    val higherNumber = BallotNumber(counter + 1, nodeUniqueId)
    val prepares = (highestCommittedIndex + 1) to (highestAcceptedIndex + 1) map {
      slot => Prepare(Identifier(nodeUniqueId, higherNumber, slot))
    }
    if (prepares.nonEmpty) prepares else Seq(Prepare(Identifier(nodeUniqueId, higherNumber, highestCommittedIndex + 1))) // FIXME empty was not picked up in unit test only when first booting a cluster
  }

  def computeFailover(log: LoggingAdapter, nodeUniqueId: Int, data: PaxosData, votes: Map[Int, PrepareResponse]): FailoverResult = {

    val largerHeartbeats: Iterable[Long] = votes.values flatMap {
      case PrepareNack(_, _, _, _, evidenceHeartbeat) if evidenceHeartbeat > data.leaderHeartbeat =>
        Some(evidenceHeartbeat)
      case _ =>
        None
    }

    lazy val largerHeartbeatCount = largerHeartbeats.size

    // here the plus one is to count the leader behind a network partition. so
    // in a three node cluster if our link to the leader goes down we will see
    // one heartbeat from the follower we can see and that plus the leader we
    // cannot see is the majority evidence of a leader behind a network partition
    def sufficientHeartbeatEvidence = largerHeartbeatCount + 1 > data.clusterSize / 2

    def noLargerHeartbeatEvidence = largerHeartbeats.isEmpty

    val decision = if (noLargerHeartbeatEvidence) {
      // all clear the last leader must be dead take over the leadership
      log.info("Node {} Follower no heartbeats executing takeover protocol.", nodeUniqueId)
      true
    } else if (sufficientHeartbeatEvidence) {
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
    val allHeartbeats = largerHeartbeats ++ Seq(data.leaderHeartbeat)
    FailoverResult(decision, allHeartbeats.max)
  }
}