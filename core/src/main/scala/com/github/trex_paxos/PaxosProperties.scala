package com.github.trex_paxos

import com.typesafe.config.Config

import scala.util.Try

case class PaxosProperties(val leaderTimeoutMin: Long, val leaderTimeoutMax: Long)

object PaxosProperties {
  val leaderTimeoutMinKey = "trex.leader-timeout-min"
  val leaderTimeoutMaxKey = "trex.leader-timeout-max"

  def apply(config: Config) = {
    /**
      * To ensure cluster stability you *must* test your max GC under extended peak loadForHighestEra and set this as some multiple
      * of observed GC pause.
      */
    val leaderTimeoutMin = Try {
      config.getInt(leaderTimeoutMinKey)
    } getOrElse (1000)

    val leaderTimeoutMax = Try {
      config.getInt(leaderTimeoutMaxKey)
    } getOrElse (3 * leaderTimeoutMin)


    require(leaderTimeoutMax > leaderTimeoutMin)

    new PaxosProperties(leaderTimeoutMin, leaderTimeoutMax)
  }

  def apply() = new PaxosProperties(1000, 3000)
}