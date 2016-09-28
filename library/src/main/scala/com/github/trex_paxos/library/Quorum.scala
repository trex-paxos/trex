package com.github.trex_paxos.library

sealed trait Outcome

case object QuorumAck extends Outcome

case object QuorumNack extends Outcome

case object SplitVote extends Outcome

trait QuorumStrategy {
  def assessPromises(promises: Iterable[PrepareResponse]): Option[Outcome]
  def assessAccepts(accepts: Iterable[AcceptResponse]): Option[Outcome]
  def promiseQuorumSize: Int
}

class DefaultQuorumStrategy(clusterSize: () => Int) extends QuorumStrategy{
  def assessPromises(promises: Iterable[PrepareResponse]): Option[Outcome] = DefaultQuorumStrategy.assessPromises(clusterSize(), promises)

  def assessAccepts(accepts: Iterable[AcceptResponse]): Option[Outcome] = DefaultQuorumStrategy.assessAccepts(clusterSize(), accepts)

  override def promiseQuorumSize: Int = clusterSize() / 2
}

object DefaultQuorumStrategy {

  def assessPromises[ResponseType](clusterSize: Int, votes: Iterable[ResponseType]): Option[Outcome] = {
    votes.toList.partition({
      case v: PrepareAck => true
      case _ => false
    }) match {
      case (positives, negatives) if positives.size > clusterSize / 2 =>
        Option(QuorumAck)
      case (positives, negatives) if negatives.size > clusterSize / 2 =>
        Option(QuorumNack)
      case (positives, negatives) if votes.size == clusterSize =>
        Option(SplitVote)
      case (positives, negatives) =>
        None
    }
  }

  def assessAccepts[ResponseType](clusterSize: Int, votes: Iterable[ResponseType]): Option[Outcome] = {
    votes.toList.partition({
      case v: AcceptAck => true
      case _ => false
    }) match {
      case (positives, negatives) if positives.size > clusterSize / 2 =>
        Option(QuorumAck)
      case (positives, negatives) if negatives.size > clusterSize / 2 =>
        Option(QuorumNack)
      case (positives, negatives) if votes.size == clusterSize =>
        Option(SplitVote)
      case (positives, negatives) =>
        None
    }
  }

}
