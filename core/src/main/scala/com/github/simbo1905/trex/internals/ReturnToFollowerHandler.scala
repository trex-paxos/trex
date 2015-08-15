package com.github.simbo1905.trex.internals

import akka.actor.ActorRef
import akka.event.LoggingAdapter

trait ReturnToFollowerHandler {
  def log: LoggingAdapter

  def randomTimeout: Long

  def nodeUniqueId: Int

  def commit(state: PaxosRole, data: PaxosData, identifier: Identifier, progress: Progress): (Progress, Seq[(Identifier, Any)])

  def sendNoLongerLeader(clientCommands: Map[Identifier, (CommandValue, ActorRef)]): Unit

  def send(actor: ActorRef, msg: Any): Unit

  def handleReturnToFollowerOnHigherCommit(c: Commit, nodeData: PaxosData, stateName: PaxosRole, sender: ActorRef): Progress = {
    log.info("Node {} {} has seen a higher commit {} from node {} so will backdown to be Follower", nodeUniqueId, stateName, c, c.identifier.from)

    val (newProgress, _) = commit(stateName, nodeData, c.identifier, nodeData.progress)

    if (newProgress == nodeData.progress) {
      send(sender, RetransmitRequest(nodeUniqueId, c.identifier.from, nodeData.progress.highestCommitted.logIndex))
    }

    sendNoLongerLeader(nodeData.clientCommands)

    newProgress
  }
}
