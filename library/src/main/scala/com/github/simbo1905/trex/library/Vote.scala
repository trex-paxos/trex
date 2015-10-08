package com.github.simbo1905.trex.library

object Vote {

  sealed trait Outcome

  case object MajorityAck extends Outcome

  case object MajorityNack extends Outcome

  case object SplitVote extends Outcome

  def count[ResponseType](clusterSize: Int, votes: Iterable[ResponseType], discriminator: (ResponseType) => Boolean): Option[Outcome] = {
    votes.toList.partition(discriminator) match {
      case (positives, negatives) if positives.size > clusterSize / 2 =>
        Option(MajorityAck)
      case (positives, negatives) if negatives.size > clusterSize / 2 =>
        Option(MajorityNack)
      case (positives, negatives) if votes.size == clusterSize =>
        Option(SplitVote)
      case (positives, negatives) =>
        None
    }
  }
}
