package com.github.simbo1905.trex.library

trait PromiseHandler[RemoteRef] extends PaxosLenses[RemoteRef] with BackdownAgent[RemoteRef] {
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
    // backdown if required
    val a = if( agent.role != Follower ) backdownAgent(io, agent) else agent
    // update promise
    val data = progressLens.set(a.data, Progress.highestPromisedLens.set(a.data.progress, prepare.id.number))
    // journal promise
    io.journal.save(data.progress)
    // ack the prepare
    io.send(PrepareAck(prepare.id, a.nodeUniqueId, data.progress, io.journal.bounds.max, data.leaderHeartbeat, io.journal.accepted(prepare.id.logIndex)))
    // retain the promise in-memory
    a.copy(data = data)
  }
}
