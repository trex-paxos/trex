package com.github.simbo1905.trex.internals

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.testkit.{TestProbe, TestFSMRef, TestKit}
import com.github.simbo1905.trex.internals.AllStateSpec._
import com.github.simbo1905.trex.internals.PaxosActor.{CheckTimeout, Configuration}
import org.scalatest.{Matchers, WordSpecLike}

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer

object TestFollowerTimeoutHandler {

  import Ordering._

  val minPrepare: Prepare = Prepare(Identifier(99, BallotNumber(Int.MinValue, Int.MinValue), Long.MinValue))

  val initialDataWithTimeoutAndPrepareResponses = PaxosData.timeoutPrepareResponsesLens.set(AllStateSpec.initialData, (99L, TreeMap(minPrepare.id -> None)) )
}

class TestFollowerTimeoutHandler extends FollowerTimeoutHandler {
  override def log: LoggingAdapter = NoopLoggingAdapter

  override def minPrepare = TestFollowerTimeoutHandler.minPrepare

  override def randomTimeout: Long = 12345L

  override def broadcast(msg: Any): Unit = ???
}

class FollowerTimeoutSpec extends TestKit(ActorSystem("FollowerTimeoutSpec", AllStateSpec.config))
with WordSpecLike with Matchers {

  import TestFollowerTimeoutHandler._

  "FollowerTimeoutHandler" should {
    "broadcast minPrepare and set a new timeout" in {
      // given a handler which records what it broadcasts
      var sentMsg: Option[Any] = None
      val handler = new TestFollowerTimeoutHandler {
        override def broadcast(msg: Any): Unit = {
          sentMsg = Option(msg)
        }
      }
      // when invoked should reset the timeout
      handler.handleResendLowPrepares(99, Follower, AllStateSpec.initialData) match {
        case data: PaxosData =>
          data.timeout shouldBe 12345L
      }
      // and broadcast the minPrepare
      sentMsg match {
        case Some(`minPrepare`) => // good
        case x => fail(s"$x")
      }
    }
  }
  "PaxosActor" should {
    "invoke the handler upon timeout" in {
      // given a node who should timeout on a low prepare
      var invoked = false
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, 3), 0, TestProbe().ref, AllStateSpec.tempFileJournal, ArrayBuffer.empty, None) {
        override def handleResendLowPrepares(nodeUniqueId: Int, stateName: PaxosRole, data: PaxosData): PaxosData = {
          invoked = true
          data
        }
        override def clock(): Long = {
          Long.MaxValue
        }
      })
      fsm.setState(Follower, initialDataWithTimeoutAndPrepareResponses)
      // when we get it to check timeout
      fsm ! CheckTimeout
      // then
      invoked shouldBe true
    }
  }
}
