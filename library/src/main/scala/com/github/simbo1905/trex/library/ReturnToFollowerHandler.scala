package com.github.simbo1905.trex.library

trait ReturnToFollowerHandler[ClientRef] extends PaxosLenses[ClientRef] with BackdownData[ClientRef] {

  def commit(io: PaxosIO[ClientRef], state: PaxosRole, data: PaxosData[ClientRef], identifier: Identifier, progress: Progress): (Progress, Seq[(Identifier, Any)])

  def handleReturnToFollowerOnHigherCommit(io: PaxosIO[ClientRef], agent: PaxosAgent[ClientRef], c: Commit): PaxosAgent[ClientRef] = {
    io.plog.info("Node {} {} has seen a higher commit {} from node {} so will backdown to be Follower", agent.nodeUniqueId, agent.role, c, c.identifier.from)

    val higherSlotCommit = c.identifier.logIndex > agent.data.progress.highestCommitted.logIndex

    val progress = if (higherSlotCommit) {
      val (newProgress, _) = commit(io, agent.role, agent.data, c.identifier, agent.data.progress)
      if (newProgress == agent.data.progress) {
        io.send(RetransmitRequest(agent.nodeUniqueId, c.identifier.from, agent.data.progress.highestCommitted.logIndex))
      }
      newProgress
    } else {
      agent.data.progress
    }

    val data = progressLens.set(agent.data, progress)

    agent.copy(data = backdownData(io, data), role = Follower)
  }
}
