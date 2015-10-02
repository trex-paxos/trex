package com.github.simbo1905.trex.library

trait ReturnToFollowerHandler[RemoteRef] extends PaxosLenses[RemoteRef] with BackdownAgent[RemoteRef] {

  def commit(io: PaxosIO[RemoteRef], agent: PaxosAgent[RemoteRef], identifier: Identifier): (Progress, Seq[(Identifier, Any)])

  def handleReturnToFollowerOnHigherCommit(io: PaxosIO[RemoteRef], agent: PaxosAgent[RemoteRef], c: Commit): PaxosAgent[RemoteRef] = {
    io.plog.info("Node {} {} has seen a higher commit {} from node {} so will backdown to be Follower", agent.nodeUniqueId, agent.role, c, c.identifier.from)

    val higherSlotCommit = c.identifier.logIndex > agent.data.progress.highestCommitted.logIndex

    val progress = if (higherSlotCommit) {
      val (newProgress, _) = commit(io, agent, c.identifier)
      if (newProgress == agent.data.progress) {
        io.send(RetransmitRequest(agent.nodeUniqueId, c.identifier.from, agent.data.progress.highestCommitted.logIndex))
      }
      newProgress
    } else {
      agent.data.progress
    }

    backdownAgent(io, agent.copy(data = progressLens.set(agent.data, progress)))
  }
}
