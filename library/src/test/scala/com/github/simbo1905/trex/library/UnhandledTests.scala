package com.github.simbo1905.trex.library

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
  }

  "UnhandledHandler" should {
    "trace the event and log an error" in {
      // given
      var loggedError: String = ""

      val handler = new UnhandledHandler[DummyRemoteRef] {
        override def stderr(message: String): Unit = loggedError = message
      }

      val unknown = "~unknown message~"

      //andler.handleUnhandled(99, Leader, probe.ref, AllStateSpec.initialData, unknown)
      handler.handleUnhandled(new TestIO(new UndefinedJournal), PaxosAgent[DummyRemoteRef](99, Leader, initialData), unknown)

      assert(loggedError.contains("99") && loggedError.contains("Leader") && loggedError.contains(unknown))
    }
  }
}
