package com.github.simbo1905.trex.internals

import akka.actor.{ActorSystem, ActorRef}
import akka.actor.FSM.Event
import akka.testkit.{TestKit, TestProbe}
import com.github.simbo1905.trex.library.{UnhandledHandler, PaxosData, Leader, PaxosRole, PaxosLogging}
import org.scalatest.WordSpecLike

class UnhandledSpec extends TestKit(ActorSystem("UnhandledSpec")) with WordSpecLike {

  class VerifiablePaxosLogging extends PaxosLogging {
    override def info(msg: String): Unit = {}

    override def error(msg: String): Unit = {}

    override def debug(msg: String, one: Any, two: Any): Unit = {}

    override def debug(msg: String, one: Any, two: Any, three: Any): Unit = {}

    override def debug(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = {}

    override def info(msg: String, one: Any): Unit = {}

    override def info(msg: String, one: Any, two: Any): Unit = {}

    override def info(msg: String, one: Any, two: Any, three: Any): Unit = {}

    override def info(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = {}
  }

  "UnhandledHandler" should {
    "trace the event and log an error" in {
      // given
      var traced = false
      var loggedError: String = ""

      val handler = new UnhandledHandler[ActorRef] {
        override def plog: PaxosLogging = new VerifiablePaxosLogging {
          override def error(message: String): Unit = loggedError = message
        }

        override def trace(state: PaxosRole, data: PaxosData[ActorRef], sender: ActorRef, msg: Any): Unit = traced = true

        override def stderr(message: String): Unit = {}
      }

      val probe = TestProbe()

      val unknown = "~unknown message~"

      handler.handleUnhandled(99, Leader, probe.ref, AllStateSpec.initialData, unknown)

      assert(traced)
      assert(loggedError.contains("99") && loggedError.contains("Leader") && loggedError.contains(unknown) )
    }
  }
}
