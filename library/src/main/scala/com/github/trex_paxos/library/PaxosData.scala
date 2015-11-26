package com.github.trex_paxos.library

import scala.collection.immutable.SortedMap

/**
 * This algebraic type holds the state of a node in the cluster.
 *
 * @param progress The highest promised and highest committed progress of a node in the cluster.
 * @param leaderHeartbeat The last heartbeat value seen from a leader. Note that clocks are not synced so this value is only used as evidence that a stable leader is up whereas the paxos number and committed slot are taken as authoritative that a new leader is making progress.
 * @param timeout The next randomised point in time that this node will timeout. Followers timeout on Commit messages and become a Recoverer. Recoverers timeout on PrepareResponses and AcceptResponses. Leaders timeout on AcceptResponses.
 * @param clusterSize The current size of the cluster.
 * @param prepareResponses The outstanding uncommitted proposed work of the leader take over phase during the recovery of a leader failover.
 *                         Each key is an identifier of a prepare for which we are collecting a majority response to determine the highest proposed value of the previous leader if any.
 * @param epoch The leaders paxos number when leading.
 * @param acceptResponses Tracking of responses to accept messages when Recoverer or Leader. Each key is an identifier of the command we want to commit. Each value is a map of the ack/nacks of each cluster node with a timeout.
 * @param clientCommands The client work outstanding with the leader. The map key is the accept identifier and the value is a tuple of the client command and the client ref.
 */
case class PaxosData(progress: Progress,
                     leaderHeartbeat: Long,
                     timeout: Long,
                     clusterSize: Int,
                     prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]] = PaxosData.emptyPrepares,
                     epoch: Option[BallotNumber] = None,
                     acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout] = PaxosData.emptyAccepts,
                     clientCommands: Map[Identifier, (CommandValue, String)] = Map.empty)

object PaxosData {
  val emptyPrepares = SortedMap.empty[Identifier, Map[Int, PrepareResponse]](Ordering.IdentifierLogOrdering)
  val emptyAccepts = SortedMap.empty[Identifier, AcceptResponsesAndTimeout](Ordering.IdentifierLogOrdering)
}