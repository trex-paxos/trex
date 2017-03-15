package com.github.trex_paxos.core

import scala.concurrent.ExecutionContext.Implicits.global

trait FixedTimeoutIO {

  def timeoutDelayMs: Long
  def timeoutPeriodMs: Long

  def scheduleFixedTimeout[T](body: => T) = {
    DelayedFuture( timeoutDelayMs, timeoutPeriodMs ){
      body
    }
  }
}
