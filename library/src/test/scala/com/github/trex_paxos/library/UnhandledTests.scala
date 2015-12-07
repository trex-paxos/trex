package com.github.trex_paxos.library

import java.util.concurrent.atomic.AtomicReference

import org.scalatest.WordSpecLike

class UnhandledTests extends WordSpecLike {

  import TestHelpers._

  "UnhandledHandler" should {
    "trace the event and log an error" in {
      // given
      val loggedError = Box("")

      val handler = new UnhandledHandler {
        override def stderr(message: String): Unit = loggedError(loggedError() +  message)
      }

      val unknown = "~unknown message~"

      handler.handleUnhandled(new TestIO(new UndefinedJournal), PaxosAgent(99, Leader, initialData), unknown)

      assert(loggedError().contains("99") && loggedError().contains("Leader") && loggedError().contains(unknown))
    }
  }
}
