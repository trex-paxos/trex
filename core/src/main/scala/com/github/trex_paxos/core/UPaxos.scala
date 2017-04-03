package com.github.trex_paxos.core

import com.github.trex_paxos.{Membership, Quorum}
import com.github.trex_paxos.library._

import scala.collection.immutable.SortedMap

object UPaxos {
  def upgradeBallotNumberToNewEra(nodeUniqueId: Int, ballotNumber: BallotNumber, progress: Progress): Prepare = {
    Prepare(Identifier(nodeUniqueId, BallotNumber.nextEra(ballotNumber), progress.highestCommitted.logIndex + 1))
  }

  def computeLeaderOverlap(nodeUniqueId: Int, membership: Membership): Overlap = {
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

    Overlap(prepareNodes, acceptNodes)
  }

  def computeNewEraPrepareOutcome(nodeUniqueId: Int, prepareNodes: Iterable[Int], quorum: Quorum, intToResponse: Map[Int, PrepareResponse]): Option[Boolean] = {

    // partition the results
    val (positives, negatives) = intToResponse.map(_.swap).partition({ case (p: PrepareNack, _) => false; case (p: PrepareAck, _) => true })

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

case class Overlap(prepareNodes: Iterable[Int], acceptNodes: Iterable[Int])

trait UPaxos {
  self: PaxosIO =>

  var newEraPrepare: Option[Prepare] = None

  var newEraOverlap: Option[Overlap] = None

  var newEraPrepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]] = PaxosData.emptyPrepares

  def reset() = {
    newEraPrepare match {
      case None => // nothing to do
      case _ =>
        newEraOverlap = None
        newEraPrepare = None
        newEraPrepareResponses = PaxosData.emptyPrepares
    }
  }

  def clearLeaderOverlapWorkIfLostLeadership(oldRole: PaxosRole, newRole: PaxosRole) = {
    if (oldRole == Leader && newRole != Leader) {
      this.logger.warning("we have lost leadership whilst upgrading our ballot to the new era")
      reset()
    }
  }

  def handleEraPrepareResponse(nodeUniqueId: Int, prepareNodes: Iterable[Int], membership: Membership, ackOrNack: PrepareResponse) = {

    def update(p: PrepareResponse) = {
      val votes = newEraPrepareResponses.getOrElse((p.requestId), Map())
      newEraPrepareResponses = newEraPrepareResponses + (p.requestId -> (votes + (p.from -> p)))
    }

    newEraPrepare match {
      case Some(eraPrepare) if eraPrepare.id == ackOrNack.requestId =>
        update(ackOrNack)
        UPaxos.computeNewEraPrepareOutcome(nodeUniqueId, prepareNodes, membership.quorumForPromises, newEraPrepareResponses(ackOrNack.requestId))
      case None => // should be unreachable
        None
    }

  }
}
