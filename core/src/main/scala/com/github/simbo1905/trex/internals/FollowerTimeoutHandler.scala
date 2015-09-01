package com.github.simbo1905.trex.internals

import akka.event.LoggingAdapter

trait FollowerTimeoutHandler {
  def log: LoggingAdapter

  def broadcast(msg: Any): Unit

  def minPrepare: Prepare

  def randomTimeout: Long

  def handleResendLowPrepares(nodeUniqueId: Int, stateName: PaxosRole, data: PaxosData): PaxosData = {
    log.debug("Node {} {} timed-out having already issued a low. rebroadcasting", nodeUniqueId, stateName)
    broadcast(minPrepare)
    PaxosData.timeoutLens.set(data, randomTimeout)
  }
}
