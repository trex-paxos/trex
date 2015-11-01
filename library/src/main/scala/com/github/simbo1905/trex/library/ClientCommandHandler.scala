package com.github.simbo1905.trex.library

import scala.collection.immutable.SortedMap
import Ordering._

trait ClientCommandHandler extends PaxosLenses {

  import ClientCommandHandler._

  def handleClientCommand(io: PaxosIO, agent: PaxosAgent, value: CommandValue, client: String): PaxosAgent = {
    val accept = acceptFor(agent, value)
    val SelfAckOrNack(response, updated) = leaderSelfAckOrNack(io, agent, accept)
    response match {
      case _: AcceptAck => io.journal.accept(accept)
      case _ => // do nothing
    }
    // respond
    io.send(accept)
    // add the sender our client map
    val clients = agent.data.clientCommands + (accept.id ->(value, client))
    agent.copy(data = leaderLens.set(agent.data, (SortedMap.empty, updated, clients)))
  }
}

case class SelfAckOrNack(response: AcceptResponse, acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout])

object ClientCommandHandler {
  def acceptFor(agent: PaxosAgent, value: CommandValue): Accept = {
    // compute next slot
    val lastLogIndex: Long = agent.data.acceptResponses.lastOption match {
      case Some((id, _)) => id.logIndex
      case _ => agent.data.progress.highestCommitted.logIndex
    }
    // create accept
    val nextLogIndex = lastLogIndex + 1
    val aid = Identifier(agent.nodeUniqueId, agent.data.epoch.get, nextLogIndex)
    Accept(aid, value)
  }

  def leaderSelfAckOrNack(io: PaxosIO, agent: PaxosAgent, accept: Accept): SelfAckOrNack = {
    val response = agent.data.progress.highestPromised match {
      case promise if promise > accept.id.number => AcceptNack(accept.id, agent.nodeUniqueId, agent.data.progress)
      case _ => AcceptAck(accept.id, agent.nodeUniqueId, agent.data.progress)
    }

    SelfAckOrNack(response, agent.data.acceptResponses + (accept.id -> AcceptResponsesAndTimeout(io.randomTimeout, accept,
      Map(agent.nodeUniqueId -> response))))
  }
}
