package com.github.trex_paxos.library

import java.util.concurrent.atomic.AtomicLong

import org.scalamock.scalatest.MockFactory
import org.scalatest.{OptionValues, Spec, Matchers}

class TestAcceptHandler extends AcceptHandler

class AcceptHandlerTests extends Spec with Matchers with MockFactory with OptionValues with PaxosLenses {

  object `A HighAcceptHandler` {
    def `should require accept number at least as high as promise` {
      val handler = new TestAcceptHandler
      val mockJournal = stub[Journal]
      val id = Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), 0)
      val agent = PaxosAgent(1, Follower, TestHelpers.initialData)
      intercept[IllegalArgumentException] {
        handler.handleHighAccept(new TestIO(mockJournal), agent, Accept(id, NoOperationCommandValue))
      }
    }
  }

  object `An AcceptHandler` {

    def `should ack an Accept equal to its promise and journal message but not save progress sending last` {
      val id = Identifier(0, TestHelpers.initialData.progress.highestPromised, 0)
      val accept = Accept(id, NoOperationCommandValue)
      val handler = new TestAcceptHandler
      val saveTs = new AtomicLong
      val journal = new UndefinedJournal {
        override def accept(a: Accept*): Unit = saveTs.set(System.nanoTime)
      }
      val agent = PaxosAgent(1, Follower, TestHelpers.initialData)
      val io = new TestIO(journal)
      handler.handleAccept(io, agent, accept)
      io.sent.headOption.value match {
        case MessageAndTimestamp(ack: AcceptAck, sendTs) if ack.requestId == id && saveTs.get < sendTs => // good
        case x => fail(x.toString)
      }
    }

    // http://stackoverflow.com/q/29880949/329496
    def `should ack an Accept higher than its promise and save new promise` {
      val highNumber = BallotNumber(Int.MaxValue, Int.MaxValue)
      val id = Identifier(0, highNumber, 1)
      val accept = Accept(id, NoOperationCommandValue)
      val saveTs = new AtomicLong()
      val journal = new UndefinedJournal {
        override def accept(a: Accept*): Unit = {}

        override def save(progress: Progress): Unit = saveTs.set(System.nanoTime)
      }
      val agent = PaxosAgent(1, Follower, TestHelpers.initialData)
      val io = new TestIO(journal)
      val handler = new TestAcceptHandler
      val PaxosAgent(_, Follower, newData) = handler.handleAccept(io, agent, accept)
      assert(saveTs != 0L)
      io.sent.headOption.value match {
        case MessageAndTimestamp(ack: AcceptAck, sendTs) if ack.requestId == id && saveTs.get < sendTs => // good
        case x => fail(x.toString)
      }
      assert(newData.progress.highestPromised == highNumber)

    }

    val expectedString = "Knossos"
    val expectedBytes = expectedString.getBytes

    def `should ack duplicated accept` {
      val stubJournal: Journal = stub[Journal]
      // given initial state
      val promised = BallotNumber(6, 1)
      val data = highestPromisedLens.set(TestHelpers.initialData, promised)
      val identifier = Identifier(0, promised, 1)
      // and some already journaled accept
      val accepted = Accept(identifier, ClientRequestCommandValue(0, expectedBytes))
      stubJournal.accepted _ when 0L returns Some(accepted)
      // when our node sees this
      val handler = new TestAcceptHandler
      val agent = PaxosAgent(1, Follower, data)
      val io = new TestIO(stubJournal)
      val PaxosAgent(_, Follower, newData) = handler.handleAccept(io, agent, accepted)
      // it acks
      io.sent.headOption.value match {
        case MessageAndTimestamp(ack: AcceptAck, _) => // good
        case x => fail(x.toString)
      }
      // and does not update any state
      newData shouldBe data
    }

    def `should nack an accept below the high watermark` {
      val stubJournal: Journal = stub[Journal]
      // given initial state
      val committedLogIndex = 1
      val promised = BallotNumber(5, 0)
      val initialData = TestHelpers.initialData.copy( progress = Progress(promised, Identifier(0, promised, committedLogIndex)))
      // and some higher identifier but for a slot already committed
      val higherIdentifier = Identifier(0, BallotNumber(6, 0), committedLogIndex)
      val acceptedAccept = Accept(higherIdentifier, ClientRequestCommandValue(0, expectedBytes))
      // when our node sees this
      val handler = new TestAcceptHandler
      val agent = PaxosAgent(1, Follower, initialData)
      val io = new TestIO(stubJournal)
      val PaxosAgent(_, Follower, newData) = handler.handleAccept(io, agent, acceptedAccept)
      // it nacks
      io.sent.headOption.value match {
        case MessageAndTimestamp(ack: AcceptNack, _) => // good
        case x => fail(x.toString)
      }
      // and does not update any state
      newData shouldBe initialData
    }

    def `sould nack and accept below current promise` {
      val id = Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), 0)
      val accept = Accept(id, NoOperationCommandValue)
      val handler = new TestAcceptHandler
      val journal = new UndefinedJournal
      val agent = PaxosAgent(1, Follower, TestHelpers.initialData)
      val io = new TestIO(journal)
      handler.handleAccept(io, agent, accept)
      io.sent.headOption.value match {
        case MessageAndTimestamp(ack: AcceptNack, _) => // good
        case x => fail(x.toString)
      }
    }
  }

}
