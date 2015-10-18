package com.github.simbo1905.trex.library

trait AcceptHandler[RemoteRef] extends PaxosLenses[RemoteRef] {

  def handleAccept(io: PaxosIO[RemoteRef], agent: PaxosAgent[RemoteRef], accept: Accept): PaxosAgent[RemoteRef] = {

    accept.id match {
      case id if id.number < agent.data.progress.highestPromised =>
        // nack lower accept
        io.send(AcceptNack(id, agent.nodeUniqueId, agent.data.progress))
        agent
      case id if id.number > agent.data.progress.highestPromised && id.logIndex <= agent.data.progress.highestCommitted.logIndex =>
        // nack higher accept for slot which is committed
        io.send(AcceptNack(id, agent.nodeUniqueId, agent.data.progress))
        agent
      case id if agent.data.progress.highestPromised <= id.number =>
        handleHighAccept(io, agent, accept)
    }

  }

  /**
   * Ack an Accept as high as promise. If the accept number > highestPromised it must update it's promise http://stackoverflow.com/q/29880949/329496
   * @param io The PaxosIO.
   * @param agent The PaxosAgent.
   * @param accept The accept required to have number greater or equal to the agent's promise.
   * @return
   */
  def handleHighAccept(io: PaxosIO[RemoteRef], agent: PaxosAgent[RemoteRef], accept: Accept): PaxosAgent[RemoteRef] = {
    require(agent.data.progress.highestPromised <= accept.id.number)
    io.journal.accept(accept)
    val updatedData = if (accept.id.number > agent.data.progress.highestPromised) {
      val dataNewPromise = highestPromisedLens.set(agent.data, accept.id.number)
      io.journal.save(dataNewPromise.progress)
      dataNewPromise
    } else {
      agent.data
    }
    io.send(AcceptAck(accept.id, agent.nodeUniqueId, agent.data.progress))
    agent.copy(data = updatedData)
  }
}
