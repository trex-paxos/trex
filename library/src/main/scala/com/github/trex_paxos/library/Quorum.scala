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

class SimplyMajorityQuorumStrategy(clusterSize: () => Int) extends QuorumStrategy {
  def assessPromises(promises: Iterable[PrepareResponse]): Option[Outcome] =
    SimpleMajorityQuorumStrategy.assessPromises(clusterSize(), promises)

  def assessAccepts(accepts: Iterable[AcceptResponse]): Option[Outcome] =
        SimpleMajorityQuorumStrategy.assessAccepts(clusterSize(), accepts)

  override def promiseQuorumSize: Int = clusterSize() / 2 + 1

}

class DefaultQuorumStrategy(clusterSize: () => Int) extends QuorumStrategy {
  def assessPromises(promises: Iterable[PrepareResponse]): Option[Outcome] = {
    SimpleMajorityQuorumStrategy.assessPromises(clusterSize(), promises)
  }

  /**
    * Here we apply the FPaxos even nodes optimisation.
    */
  def assessAccepts(accepts: Iterable[AcceptResponse]): Option[Outcome] = {
    val size = clusterSize()
    size % 2 match {
      case 0 =>
        SimpleMajorityQuorumStrategy.assessAccepts(size - 1, accepts)
      case _ =>
        SimpleMajorityQuorumStrategy.assessAccepts(size, accepts)
    }
  }

  override def promiseQuorumSize: Int = clusterSize() / 2 + 1

}

object DefaultQuorumStrategy {
  def apply(clusterSize: () => Int) = new DefaultQuorumStrategy(clusterSize)
}

object SimpleMajorityQuorumStrategy {

  def simpleMajority(clusterSize: Int, positives: Int, negatives: Int) = {
    (positives, negatives) match {
    case (p, _) if p > clusterSize / 2 =>
      Option(QuorumAck)
    case (_, n) if n > clusterSize / 2 =>
      Option(QuorumNack)
    case (p, n) if p + n == clusterSize =>
      Option(SplitVote)
    case (_, _) =>
      None
    }
  }

  def assessPromises[ResponseType](clusterSize: Int, votes: Iterable[ResponseType]): Option[Outcome] = {
    votes.toList.partition({
      case v: PrepareAck => true
      case _ => false
    }) match {
      case (positives, negatives) => simpleMajority(clusterSize, positives.size, negatives.size)
    }
  }

  def assessAccepts[ResponseType](clusterSize: Int, votes: Iterable[ResponseType]): Option[Outcome] = {
    votes.toList.partition({
      case v: AcceptAck => true
      case _ => false
    }) match {
      case (positives, negatives) => simpleMajority(clusterSize, positives.size, negatives.size)
    }
  }

}
