package com.github.simbo1905.trex.library

import org.scalamock.scalatest.MockFactory
import org.scalatest.{OptionValues, Spec}

class TestHighAcceptHandler extends HighAcceptHandler[DummyRemoteRef]

class HighAcceptHandlerTests extends Spec with MockFactory with OptionValues {

  object `A HighAcceptHandler` {
    def `should require accept number at least as high as promise` {
      val handler = new TestHighAcceptHandler
      val mockJournal = stub[Journal]
      val id = Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), 0)
      val agent = PaxosAgent(1, Follower, TestHelpers.initialData)
      intercept[IllegalArgumentException] {
        handler.handleHighAccept(new TestIO(mockJournal), agent, Accept(id, NoOperationCommandValue))
      }
    }

    def `should ack an Accept equal to its promise and journal message but not save progress sending last` {
      val id = Identifier(0, TestHelpers.initialData.progress.highestPromised, 0)
      val accept = Accept(id, NoOperationCommandValue)
      val handler = new TestHighAcceptHandler
      var saveTs = 0L
      val journal = new UndefinedJournal {
        override def accept(a: Accept*): Unit = saveTs = System.nanoTime
      }
      val agent = PaxosAgent(1, Follower, TestHelpers.initialData)
      val io = new TestIO(journal)
      handler.handleHighAccept(io, agent, accept)
      io.sent.headOption.value match {
        case MessageAndTimestamp(ack: AcceptAck, sendTs) if ack.requestId == id && saveTs < sendTs => // good
        case x => fail(x.toString)
      }
    }

    def `should ack an Accept higher than its promise and save new promise` {
      val highNumber = BallotNumber(Int.MaxValue, Int.MaxValue)
      val id = Identifier(0, highNumber, 0)
      val accept = Accept(id, NoOperationCommandValue)
      val handler = new TestHighAcceptHandler
      var saveTs = 0L
      val journal = new UndefinedJournal {
        override def accept(a: Accept*): Unit = {}

        override def save(progress: Progress): Unit = saveTs = System.nanoTime
      }
      val agent = PaxosAgent(1, Follower, TestHelpers.initialData)
      val io = new TestIO(journal)
      val PaxosAgent(_, Follower, newData) = handler.handleHighAccept(io, agent, accept)
      assert(saveTs != 0L)
      io.sent.headOption.value match {
        case MessageAndTimestamp(ack: AcceptAck, sendTs) if ack.requestId == id && saveTs < sendTs => // good
        case x => fail(x.toString)
      }
      assert(newData.progress.highestPromised == highNumber)

    }
  }

}
