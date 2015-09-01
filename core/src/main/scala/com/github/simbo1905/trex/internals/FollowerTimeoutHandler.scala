package com.github.simbo1905.trex.internals

import akka.event.LoggingAdapter

import scala.collection.SortedMap

import Ordering._

trait FollowerTimeoutHandler {
  def log: LoggingAdapter

  def broadcast(msg: Any): Unit

  def minPrepare: Prepare

  def randomTimeout: Long

  def highestAcceptedIndex: Long

  def handleResendLowPrepares(nodeUniqueId: Int, stateName: PaxosRole, data: PaxosData): PaxosData = {
    log.debug("Node {} {} timed-out having already issued a low. rebroadcasting", nodeUniqueId, stateName)
    broadcast(minPrepare)
    PaxosData.timeoutLens.set(data, randomTimeout)
  }

  def handleFollowerTimeout(nodeUniqueId: Int, stateName: PaxosRole, data: PaxosData): PaxosData = {
    log.info("Node {} {} timed-out progress: {}", nodeUniqueId, stateName, data.progress)
    broadcast(minPrepare)
    // nak our own prepare
    val prepareSelfVotes = SortedMap.empty[Identifier, Option[Map[Int, PrepareResponse]]] ++
      Map(minPrepare.id -> Some(Map(nodeUniqueId -> PrepareNack(minPrepare.id, nodeUniqueId, data.progress, highestAcceptedIndex, data.leaderHeartbeat))))

    PaxosData.timeoutPrepareResponsesLens.set(data, (randomTimeout, prepareSelfVotes))
  }
}
