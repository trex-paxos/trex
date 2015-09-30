package com.github.simbo1905.trex.library

trait HighAcceptHandler[ClientRef] extends PaxosLenses[ClientRef] {
  /**
   * Ack an Accept as high as promise. If the accept number > highestPromised it must update it's promise http://stackoverflow.com/q/29880949/329496
   * @param io The PaxosIO.
   * @param agent The PaxosAgent.
   * @param accept The accept required to have number greater or equal to the agent's promise.
   * @return
   */
  def handleHighAccept(io: PaxosIO[ClientRef], agent: PaxosAgent[ClientRef], accept: Accept): PaxosAgent[ClientRef] = {
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
