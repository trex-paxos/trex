package com.github.simbo1905.trex.library

import scala.collection.immutable.SortedMap
import Ordering._

trait ClientCommandHandler[ClientRef] extends PaxosLenses[ClientRef] {
  def handleClientCommand(io: PaxosIO[ClientRef], agent: PaxosAgent[ClientRef], value: CommandValue, client: ClientRef): PaxosAgent[ClientRef] = {
    agent.data.epoch match {
      // the following 'if' check is an invariant of the algorithm we will throw and kill the actor if we have no match
      case Some(epoch) if agent.data.progress.highestPromised <= epoch =>
        // compute next slot
        val lastLogIndex: Long = agent.data.acceptResponses.lastOption match {
          case Some((id, _)) => id.logIndex
          case _ => agent.data.progress.highestCommitted.logIndex
        }
        // create accept
        val nextLogIndex = lastLogIndex + 1
        val aid = Identifier(agent.nodeUniqueId, agent.data.epoch.get, nextLogIndex)
        val accept = Accept(aid, value)

        // self accept
        io.journal.accept(accept)
        // register self
        val updated = agent.data.acceptResponses + (aid -> AcceptResponsesAndTimeout(io.randomTimeout, accept,
          Map(agent.nodeUniqueId -> AcceptAck(aid, agent.nodeUniqueId, agent.data.progress))))
        // broadcast
        io.send(accept)
        // add the sender our client map
        val clients = agent.data.clientCommands + (accept.id ->(value, client))
        agent.copy(data = leaderLens.set(agent.data, (SortedMap.empty, updated, clients)))
      case x =>
        throw new AssertionError(s"Invariant violation as '$x' does not match case Some(epoch) if ${agent.data.progress.highestPromised} <= epoch")
    }

  }
}
