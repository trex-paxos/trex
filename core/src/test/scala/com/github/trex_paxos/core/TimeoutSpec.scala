package com.github.trex_paxos.core

import com.github.trex_paxos.PaxosProperties
import com.github.trex_paxos.library.{Box, CheckTimeout, HeartBeat, PaxosMessage}
import org.scalatest.{Matchers, OptionValues, WordSpecLike}

import scala.collection.mutable.ArrayBuffer
import scala.compat.Platform

class TimeoutSpec extends WordSpecLike with Matchers with OptionValues {

  abstract class TestRandomTimeoutIO extends RandomTimeoutIO with SingletonActor[PaxosMessage, Seq[PaxosMessage]]  {
    override protected def receive(rq: PaxosMessage): Seq[PaxosMessage] = Seq()
  }

  "RandomTimeoutIO " should {
    "issue randoms in the correct range" in {
      val props = PaxosProperties(100, 300)
      val testTimeoutIO = new TestRandomTimeoutIO{
        override def paxosProperties: PaxosProperties = props
      }
      (1 to 1024) foreach { _ =>
        val r = testTimeoutIO.randomInterval
        assert( r >= props.leaderTimeoutMin && r <= props.leaderTimeoutMax)
      }
    }
    "schedules a CheckTimeout message in the future" in {
      val props = PaxosProperties(1, 2)
      val msgs = ArrayBuffer[PaxosMessage]()
      val testTimeoutIO = new TestRandomTimeoutIO{
        override def paxosProperties: PaxosProperties = props

        override protected def receive(rq: PaxosMessage): Seq[PaxosMessage] = {
          msgs synchronized {
            msgs += rq
            msgs.notifyAll()
          }
          Seq()
        }
      }

      testTimeoutIO.scheduleRandomCheckTimeout()

      msgs synchronized {
        if( msgs.isEmpty) msgs.wait(1000)
      }

      assert(!msgs.isEmpty && msgs.headOption.value == CheckTimeout)
    }
  }
  "Heartbeat" should {
    "schedule a repeated heartbeat" in {
      val msgs = ArrayBuffer[PaxosMessage]()
      val hb = new Heatbeat with SingletonActor[PaxosMessage, Seq[PaxosMessage]] {
        override def paxosProperties: PaxosProperties = PaxosProperties(10, 30)

        override protected def receive(rq: PaxosMessage): Seq[PaxosMessage] = {
          msgs synchronized {
            msgs += rq
            if( msgs.size >= 2) msgs.notifyAll()
          }
          Seq()
        }
      }

      msgs synchronized {
        if( msgs.size < 2 ) msgs.wait(1000)
      }

      assert(msgs.size >= 2 && msgs.headOption.value == HeartBeat)
    }
  }
  "FixedTimeoutIO" should {
    "schedule a repeated message" in {
      val testFixedIO = new FixedTimeoutIO  {
        val timeoutPeriodMs: Long = 1

        val timeoutDelayMs: Long = 10
      }
      val times = ArrayBuffer[Long]()

      // log start time
      times += Platform.currentTime

      // schedule repeated logging of times
      testFixedIO.scheduleFixedTimeout {
        times synchronized {
          times += Platform.currentTime
          if( times.size >= 4) times.notifyAll()
        }
      }

      times synchronized {
        if( times.size < 4) times.wait(100)
      }

      // we should have seen repeated times
      assert( times.size >= 4)
      // there should be a minimum delay of 10ms for the first repeat.
      assert( times(1) - times(0) >= 10)
    }
  }
}
