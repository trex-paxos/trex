package com.github.trex_paxos.core

import com.github.trex_paxos.PaxosProperties
import com.github.trex_paxos.library.{HeartBeat, PaxosMessage}

trait Heatbeat { self: SingletonActor[PaxosMessage, Seq[PaxosMessage]] =>

  def paxosProperties: PaxosProperties

  /**
    * Override this to a value that appropriately suppresses follower timeouts
    */
  def heartbeatInterval = Math.max(paxosProperties.leaderTimeoutMin / 3, 1)

  {
    import scala.concurrent.ExecutionContext.Implicits.global
    DelayedFuture( paxosProperties.leaderTimeoutMin, heartbeatInterval ){
      self ? HeartBeat
    }
  }
}

