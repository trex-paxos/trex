package com.github.trex_paxos.core

import com.github.trex_paxos.library._

import scala.collection.immutable.SortedMap

trait PaxosEngine
  extends PaxosIO
  with CollectingPaxosIO
  with RandomTimeoutIO
  with SingletonActor[PaxosMessage, Seq[PaxosMessage]] {

  def journal: Journal

  def initialAgent: PaxosAgent

  private[this] var agent = initialAgent

  private val paxosAlgorithm = new PaxosAlgorithm

  override protected def receive(message: PaxosMessage): Seq[PaxosMessage] = {
    require(logger != null)
    logger.info("receive: {}", message)
    outbound.clear()
    val oldData = agent.data
    val event = PaxosEvent(this, agent, message)
    agent = paxosAlgorithm(event)
    logger.debug("|{}|{}|{}|{}|", agent.nodeUniqueId, oldData, message, agent.data, outbound)
    collection.immutable.Seq[PaxosMessage](outbound:_*)
  }
}

object PaxosEngine {
  def initialAgent(nodeUniqueId: Int, progress: Progress, clusterSize: () => Int) = new PaxosAgent(nodeUniqueId, Follower, PaxosData(progress, 0, 0,
    SortedMap.empty[Identifier, scala.collection.immutable.Map[Int, PrepareResponse]](Ordering.IdentifierLogOrdering), None,
    SortedMap.empty[Identifier, AcceptResponsesAndTimeout](Ordering.IdentifierLogOrdering)
  ), DefaultQuorumStrategy(clusterSize))
}
