package com.github.simbo1905.trex.internals

import akka.actor.{ActorSystem, ActorRef}
import akka.actor.FSM.Event
import akka.event.LoggingAdapter
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.WordSpecLike

class VerifiableUnhandledHandler extends LoggingAdapter {
  override def isErrorEnabled: Boolean = false

  override protected def notifyInfo(message: String): Unit = {}

  override def isInfoEnabled: Boolean = false

  override def isDebugEnabled: Boolean = false

  override protected def notifyError(message: String): Unit = {}

  override protected def notifyError(cause: Throwable, message: String): Unit = {}

  override def isWarningEnabled: Boolean = false

  override protected def notifyWarning(message: String): Unit = {}

  override protected def notifyDebug(message: String): Unit = {}
}

class UnhandledSpec extends TestKit(ActorSystem("UnhandledSpec")) with WordSpecLike {
  "UnhandledHandler" should {
    "trace the event and log an error" in {
      // given
      var traced = false
      var loggedError: String = ""

      val handler = new UnhandledHandler {
        override def log: LoggingAdapter = new VerifiableUnhandledHandler {
          override def error(message: String): Unit = loggedError = message
        }

        override def trace(state: PaxosRole, data: PaxosData, sender: ActorRef, msg: Any): Unit = traced = true

        override def stderr(message: String): Unit = {}
      }

      val probe = TestProbe()

      val unknown = "~unknown message~"

      handler.handleUnhandled(99, Leader, probe.ref, Event(unknown, AllStateSpec.initialData))

      assert(traced)
      assert(loggedError.contains("99") && loggedError.contains("Leader") && loggedError.contains(unknown) )
    }
  }
}
