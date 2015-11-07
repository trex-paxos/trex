package com.github.trex_paxos.library

trait ReturnToFollowerHandler extends PaxosLenses with BackdownAgent {

  def commit(io: PaxosIO, agent: PaxosAgent, identifier: Identifier): (Progress, Seq[(Identifier, Any)])

  /**
   * If we see a commit at a higher slot we should backdown and request retransmission.
   * If we see a commit for the same slot but with a higher epoch id we should backdown.
   * Other commits are ignored.
   */
  def handleReturnToFollowerOnHigherCommit(io: PaxosIO, agent: PaxosAgent, c: Commit): PaxosAgent = {

    val higherSlotCommit = c.identifier.logIndex > agent.data.progress.highestCommitted.logIndex
    lazy val equalCommit = c.identifier.logIndex == agent.data.progress.highestCommitted.logIndex
    lazy val higherNumberCommit = c.identifier.number > agent.data.epoch.getOrElse(Journal.minNumber)

    if (higherSlotCommit || (equalCommit && higherNumberCommit)) {
      val newProgress = if( higherSlotCommit ){
        val (progress, _) = commit(io, agent, c.identifier)
        if (progress == agent.data.progress) {
          io.send(RetransmitRequest(agent.nodeUniqueId, c.identifier.from, agent.data.progress.highestCommitted.logIndex))
        }
        progress
      } else {
        agent.data.progress
      }

      io.plog.info("Node {} {} has seen a higher commit {} from node {} so will backdown to be Follower", agent.nodeUniqueId, agent.role, c, c.identifier.from)
      backdownAgent(io, agent.copy(data = progressLens.set(agent.data, newProgress).copy(leaderHeartbeat = c.heartbeat)))
    } else {
      agent
    }
  }
}
