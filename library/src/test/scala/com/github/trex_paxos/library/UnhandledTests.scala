package com.github.trex_paxos.library

import java.util.concurrent.atomic.AtomicReference

import org.scalatest.WordSpecLike

class UnhandledTests extends WordSpecLike {

  import TestHelpers._

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

    override def warning(msg: String): Unit = {}

    override def warning(msg: String, one: Any, two: Any): Unit = {}
  }

  "UnhandledHandler" should {
    "trace the event and log an error" in {
      // given
      val loggedError = new AtomicReference[String]("")

      val handler = new UnhandledHandler {
        override def stderr(message: String): Unit = loggedError.set(loggedError.get +  message)
      }

      val unknown = "~unknown message~"

      //andler.handleUnhandled(99, Leader, probe.ref, AllStateSpec.initialData, unknown)
      handler.handleUnhandled(new TestIO(new UndefinedJournal), PaxosAgent(99, Leader, initialData), unknown)

      assert(loggedError.get.contains("99") && loggedError.get.contains("Leader") && loggedError.get.contains(unknown))
    }
  }
}
