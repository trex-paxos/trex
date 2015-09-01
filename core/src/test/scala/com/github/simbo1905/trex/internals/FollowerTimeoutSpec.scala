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

  val initialDataWithTimeoutAndPrepareResponses = PaxosData.timeoutPrepareResponsesLens.set(AllStateSpec.initialData, (99L, TreeMap(minPrepare.id -> None)))
}

class TestFollowerTimeoutHandler extends FollowerTimeoutHandler {
  override def log: LoggingAdapter = NoopLoggingAdapter

  override def minPrepare = TestFollowerTimeoutHandler.minPrepare

  override def randomTimeout: Long = 12345L

  override def broadcast(msg: Any): Unit = ???

  override def highestAcceptedIndex: Long = 0L
}

class FollowerTimeoutSpec extends TestKit(ActorSystem("FollowerTimeoutSpec", AllStateSpec.config))
with WordSpecLike with Matchers {

  import TestFollowerTimeoutHandler._

  "FollowerTimeoutHandler" should {
    "broadcast a minPrepare and set a new timeout" in {
      // given a handler which records what it broadcasts
      var sentMsg: Option[Any] = None
      val handler = new TestFollowerTimeoutHandler {
        override def broadcast(msg: Any): Unit = {
          sentMsg = Option(msg)
        }
      }
      // when timeout on minPrepare logic is invoked
      handler.handleFollowerTimeout(99, Follower, AllStateSpec.initialData) match {
        case data: PaxosData =>
          // it should set a fresh timeout
          data.timeout shouldBe 12345L
          // and have nacked its own minPrepare
          data.prepareResponses.get(minPrepare.id) match {
            case Some(Some(map)) =>
              map.get(99) match {
                case Some(nack: PrepareNack) => // good
                case x => fail(s"$x")
              }
            // good
            case x => fail(s"$x")
          }
      }
      // and have broadcast the minPrepare
      sentMsg match {
        case Some(`minPrepare`) => // good
        case x => fail(s"$x")
      }
    }
    "rebroadcast minPrepare and set a new timeout" in {
      // given a handler which records what it broadcasts
      var sentMsg: Option[Any] = None
      val handler = new TestFollowerTimeoutHandler {
        override def broadcast(msg: Any): Unit = {
          sentMsg = Option(msg)
        }
      }
      // when timeout on minPrepare logic is invoked
      handler.handleResendLowPrepares(99, Follower, AllStateSpec.initialData) match {
        case data: PaxosData =>
          // it should set a fresh timeout
          data.timeout shouldBe 12345L
      }
      // and have broadcast the minPrepare
      sentMsg match {
        case Some(`minPrepare`) => // good
        case x => fail(s"$x")
      }
    }
  }
  "PaxosActor" should {
    "invoke the follower timeout handler" in {
      // given a node who should timeout on a low prepare
      var invoked = false
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, 3), 0, TestProbe().ref, AllStateSpec.tempFileJournal, ArrayBuffer.empty, None) {
        override def handleFollowerTimeout(nodeUniqueId: Int, stateName: PaxosRole, data: PaxosData): PaxosData = {
          invoked = true
          super.handleFollowerTimeout(nodeUniqueId, stateName, data)
        }

        override def clock(): Long = {
          Long.MaxValue
        }

      })
      fsm.setState(Follower, AllStateSpec.initialData)
      // when we get it to check timeout
      fsm ! CheckTimeout
      // then
      invoked shouldBe true
    }
    "invoke the resend low prepare handler upon timeout" in {
      // given a node who should timeout on a low prepare
      var invoked = false
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, 3), 0, TestProbe().ref, AllStateSpec.tempFileJournal, ArrayBuffer.empty, None) {
        override def handleResendLowPrepares(nodeUniqueId: Int, stateName: PaxosRole, data: PaxosData): PaxosData = {
          invoked = true
          super.handleResendLowPrepares(nodeUniqueId, stateName, data)
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
