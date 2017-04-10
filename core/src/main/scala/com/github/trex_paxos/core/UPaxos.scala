package com.github.trex_paxos.core

import com.github.trex_paxos.{ClusterConfiguration, Quorum}
import com.github.trex_paxos.library._

import scala.collection.immutable.SortedMap

object UPaxos {
  /**
    * Issues a prepare for the next slot with a new ballot number in the next era
    *
    * @param nodeUniqueId The current nodes unique id
    * @param ballotNumber The current nodes leading ballot number
    * @param progress     The current nodes progress.
    */
  def upgradeBallotNumberToNewEra(nodeUniqueId: Int, ballotNumber: BallotNumber, progress: Progress): Prepare = {
    Prepare(Identifier(nodeUniqueId, BallotNumber.nextEra(ballotNumber), progress.highestCommitted.logIndex + 1))
  }

  /**
    * This method partitions the cluster into two sets. An Phase 1 prepare set and a Phase II accept quorum.
    * The current nodes will be the casting vote to achieve a quorum in each group. Validation of whether
    * the leader is able to give the casting vote should be done before the cluster accepts the new configuration.
    *
    * @param nodeUniqueId The current nodes unique Id
    * @param membership   The current cluster membership
    * @return The two cluster groups.
    */
  def computeLeaderOverlap(nodeUniqueId: Int, membership: ClusterConfiguration): LeaderOverlap = {
    val ordered = membership.nodes.map(_.nodeIdentifier).toList.sortWith({
      _.compareTo(_) < 0
    })
    val others = ordered.filterNot(_ == nodeUniqueId)

    def sufficientWeights(quorum: Quorum, others: List[Int]): Set[Int] = {
      val weights = quorum.of.map({ w => (w.nodeIdentifier, w.weight) }).toMap
      val required = quorum.count - weights(nodeUniqueId)

      case class Sum(votes: Int, nodes: Set[Int] = Set())

      val Sum(_, nodes) = others.foldLeft(Sum(0)) {
        case (Sum(total, set), id) if total < required =>
          Sum(total + weights(id), set + id)
        case (Sum(_, set), _) => return set
      }
      nodes
    }

    val prepareNodes = sufficientWeights(membership.quorumForPromises, others)
    val acceptNodes = sufficientWeights(membership.quorumForPromises, others.reverse)

    LeaderOverlap(prepareNodes, acceptNodes)
  }

  /**
    * This method computes whether sufficient prepare responses have been received for the current node to give the
    * casting vote for the prepare message of the new era.
    *
    * @param nodeUniqueId            The current nodes unique Id
    * @param prepareNodes            The set of nodes in the Phase I prepare set.
    * @param quorum                  The prepare quorum
    * @param nodeIndexToNodeResponse The map of node responses seen so far.
    * @return None if more votes needed. Some(true) for success and Some(false) for failure (too many negative votes).
    */
  def computeNewEraPrepareOutcome(nodeUniqueId: Int, prepareNodes: Iterable[Int], quorum: Quorum, nodeIndexToNodeResponse: Map[Int, PrepareResponse]): Option[Boolean] = {

    // partition the results
    val (positives, negatives) = nodeIndexToNodeResponse.map(_.swap).partition({ case (p: PrepareNack, _) => false; case (p: PrepareAck, _) => true })

    // a lookup table of nodes to weights
    val weights = quorum.of.map({ w => (w.nodeIdentifier, w.weight) }).toMap

    // the amount of positive votes needed excluding the current node
    val othersVotesNeeded = quorum.count - weights(nodeUniqueId)

    // the amount of positive votes achieved
    val positiveCount = positives.values.map(weights(_)).sum

    if (positiveCount >= othersVotesNeeded) {
      Option(true)
    } else {
      // we will ask ourself last so our own vote is up for grabs
      val willVote = prepareNodes.toSet + nodeUniqueId
      // we didn't poll all users to the max amount of vote is less than the full quorum
      val votesPossible = weights.filter({ case (n, w) => willVote.contains(n) }).values.sum
      // the amount of negative votes achieved
      val negativesCount = negatives.values.map(weights(_)).sum
      // the total votes we are can achieve given who we polled, that we will vote last, and negatives seen
      val achievablePositivesCount = votesPossible - negativesCount
      if (achievablePositivesCount < quorum.count) {
        Option(false)
      } else {
        None
      }
    }
  }
}

/**
  * Two sets of nodes not including the leader which should be the casting vote in the prepare group and the overlap
  * nodee in the accept group.
  */
case class LeaderOverlap(prepareNodes: Iterable[Int], acceptNodes: Iterable[Int])

/**
  * An algebraic type to track the state of the leader overlap mode.
  *
  * @param newEraPrepare          The prepare message issued for the new era.
  * @param newEraOverlap          The two sets of nodes one sent the the new era prepare and the other sent any further accept messages.
  * @param newEraPrepareResponses Collects the prepare responses to the new ear prepare.
  */
case class UPaxosContext(newEraPrepare: Prepare, newEraOverlap: LeaderOverlap, newEraPrepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]] = PaxosData.emptyPrepares)

/**
  * UPaxos leader casting vote logic.
  */
trait UPaxos {
  self: PaxosIO =>

  /**
    * The state used to track the leader casting vote.
    */
  var uPaxosContext: Option[UPaxosContext] = None

  /**
    * Resets the state which happens if we get either a successful or failed outcome.
    */
  def reset() = uPaxosContext = None

  /**
    * If we lost the leadership reset.
    */
  def clearLeaderOverlapWorkIfLostLeadership(oldRole: PaxosRole, newRole: PaxosRole) = {
    if (oldRole == Leader && newRole != Leader) {
      this.logger.warning("we have lost leadership whilst upgrading our ballot to the new era")
      reset()
    }
  }

  /**
    * Collects and processes the responses to the new era prepare message.
    *
    * @param nodeUniqueId The current nodes unique id.
    * @param prepareNodes The nodes sent the new era prepare message.
    * @param membership   The current cluster membership.
    * @param ackOrNack    The next response to the new era prepare to handle.
    * @return None if more votes needed. Some(true) for success and Some(false) for failure (too many negative votes).
    */
  def handleEraPrepareResponse(nodeUniqueId: Int, prepareNodes: Iterable[Int], membership: ClusterConfiguration, ackOrNack: PrepareResponse): Option[Boolean] = {
    uPaxosContext match {
      case Some(context@UPaxosContext(eraPrepare, _, responses)) if eraPrepare.id == ackOrNack.requestId =>
        val votes = responses.getOrElse((eraPrepare.id), Map())
        val newResponses = responses + (eraPrepare.id -> (votes + (ackOrNack.from -> ackOrNack)))
        uPaxosContext = Option(context.copy(newEraPrepareResponses = newResponses))
        UPaxos.computeNewEraPrepareOutcome(nodeUniqueId, prepareNodes, membership.quorumForPromises, newResponses(ackOrNack.requestId))
      case None => // should be unreachable
        None
    }
  }
}
