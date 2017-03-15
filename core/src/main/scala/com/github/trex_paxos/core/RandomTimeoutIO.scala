package com.github.trex_paxos.core

import java.security.SecureRandom
import java.util.concurrent.TimeUnit

import com.github.trex_paxos.PaxosProperties
import com.github.trex_paxos.library.{CheckTimeout, PaxosMessage}

import scala.compat.Platform
import scala.concurrent.duration.FiniteDuration
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global

trait RandomTimeoutIO { self: SingletonActor[PaxosMessage, Seq[PaxosMessage]] =>

  val secureRandom = new SecureRandom()

  def random: Random = secureRandom

  def clock() = Platform.currentTime

  def paxosProperties: PaxosProperties

  def randomInterval: Long = {
    val properties = paxosProperties
    val min = properties.leaderTimeoutMin
    val max = properties.leaderTimeoutMax
    val range = max - min
    require(min > 0)
    require(max > 0)
    require(range > 0)
    min + (range * random.nextDouble()).toLong
  }

  def scheduleRandomCheckTimeout() = {
    val randomDelay = randomInterval
    DelayedFuture( FiniteDuration(randomDelay, TimeUnit.MILLISECONDS) ){
      self ? CheckTimeout
//      map {
//        msgs => transmitMessages(msgs)
//      }
    }
    clock() + randomDelay
  }
}
