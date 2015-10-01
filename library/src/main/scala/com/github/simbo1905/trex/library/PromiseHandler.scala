package com.github.simbo1905.trex.library

trait PromiseHandler[RemoteRef] extends PaxosLenses[RemoteRef] with BackdownData[RemoteRef] {
  /**
   * Makes a higher promise, journals it and responds to the sender with a PrepareAck.
   * Returns to follower if the agent was not already a follower.
   * If the agent was a leader send out NoLongerLeader messages to any clients.
   * @param io The IO operations.
   * @param agent The current agent.
   * @param prepare The message.
   * @return The updated agent.
   */
  def handlePromise(io:PaxosIO[RemoteRef], agent: PaxosAgent[RemoteRef], prepare: Prepare): PaxosAgent[RemoteRef] = {
    require(prepare.id.number > agent.data.progress.highestPromised)
    val followerData = backdownData(io, agent.data)
    val data = progressLens.set(followerData, Progress.highestPromisedLens.set(agent.data.progress, prepare.id.number))
    io.journal.save(data.progress)
    io.send(PrepareAck(prepare.id, agent.nodeUniqueId, data.progress, io.journal.bounds.max, data.leaderHeartbeat, io.journal.accepted(prepare.id.logIndex)))
    agent.copy(data = data, role = Follower)
  }
}
