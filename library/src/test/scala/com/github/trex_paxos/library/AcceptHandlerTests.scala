package com.github.trex_paxos.library

import java.util.concurrent.atomic.AtomicLong

import org.scalamock.scalatest.MockFactory
import org.scalatest.refspec.RefSpec

import org.scalatest._
import matchers.should._

class TestAcceptHandler extends AcceptHandler

class AcceptHandlerTests extends RefSpec with Matchers
  with MockFactory
  with OptionValues with PaxosLenses {

  object `A HighAcceptHandler` {
    def `should require accept number at least as high as promise`(): Unit = {
      val handler = new TestAcceptHandler
      val mockJournal = stub[Journal]
      val id = Identifier(0, BallotNumber(0, 0), 0)
      val agent = PaxosAgent(1, Follower, TestHelpers.initialData, TestHelpers.initialQuorumStrategy)
      intercept[IllegalArgumentException] {
        handler.handleHighAccept(new TestIO(mockJournal), agent, Accept(id, NoOperationCommandValue))
      }
    }
  }

  object `An AcceptHandler` {

    def `should ack an Accept equal to its promise and journal message but not save progress sending last`(): Unit = {
      val id = Identifier(0, TestHelpers.initialData.progress.highestPromised, 0)
      val accept = Accept(id, NoOperationCommandValue)
      val handler = new TestAcceptHandler
      val saveTs = new AtomicLong
      val journal = new UndefinedJournal {
        override def accept(a: Accept*): Unit = saveTs.set(System.nanoTime)
      }
      val agent = PaxosAgent(1, Follower, TestHelpers.initialData, TestHelpers.initialQuorumStrategy)
      val io = new TestIO(journal)
      handler.handleAccept(io, agent, accept)
      io.sent().headOption.value match {
        case MessageAndTimestamp(ack: AcceptAck, sendTs) if ack.requestId == id && saveTs.longValue() < sendTs => // good
        case x => fail(x.toString)
      }
    }

    // http://stackoverflow.com/q/29880949/329496
    def `should ack an Accept higher than its promise and save new promise`(): Unit = {
      val highNumber = BallotNumber(Int.MaxValue, Int.MaxValue)
      val id = Identifier(0, highNumber, 1)
      val accept = Accept(id, NoOperationCommandValue)
      val saveTs = new AtomicLong()
      val journal = new UndefinedJournal {
        override def accept(a: Accept*): Unit = {}

        override def saveProgress(progress: Progress): Unit = saveTs.set(System.nanoTime)
      }
      val agent = PaxosAgent(1, Follower, TestHelpers.initialData, TestHelpers.initialQuorumStrategy)
      val io = new TestIO(journal)
      val handler = new TestAcceptHandler
      val PaxosAgent(_, Follower, newData, _) = handler.handleAccept(io, agent, accept)
      assert(saveTs != 0L)
      io.sent().headOption.value match {
        case MessageAndTimestamp(ack: AcceptAck, sendTs) if ack.requestId == id && saveTs.longValue() < sendTs => // good
        case x => fail(x.toString)
      }
      assert(newData.progress.highestPromised == highNumber)

    }

    val expectedString = "Knossos"
    val expectedBytes = expectedString.getBytes

    def `should ack duplicated accept`(): Unit = {
      val stubJournal: Journal = stub[Journal]
      // given initial state
      val promised = BallotNumber(6, 1)
      val data = highestPromisedLens.set(TestHelpers.initialData, promised)
      val identifier = Identifier(0, promised, 1)
      // and some already journaled accept
      val accepted = Accept(identifier, DummyCommandValue("1"))
      stubJournal.accepted _ when 0L returns Some(accepted)
      // when our node sees this
      val handler = new TestAcceptHandler
      val agent = PaxosAgent(1, Follower, data, TestHelpers.initialQuorumStrategy)
      val io = new TestIO(stubJournal)
      val PaxosAgent(_, Follower, newData, _) = handler.handleAccept(io, agent, accepted)
      // it acks
      io.sent().headOption.value match {
        case MessageAndTimestamp(ack: AcceptAck, _) => // good
        case x => fail(x.toString)
      }
      // and does not update any state
      newData shouldBe data
    }

    def `should nack an accept below the high watermark`(): Unit = {
      val stubJournal: Journal = stub[Journal]
      // given initial state
      val committedLogIndex = 1
      val promised = BallotNumber(5, 0)
      val initialData = TestHelpers.initialData.copy( progress = Progress(promised, Identifier(0, promised, committedLogIndex)))
      // and some higher identifier but for a slot already committed
      val higherIdentifier = Identifier(0, BallotNumber(6, 0), committedLogIndex)
      val acceptedAccept = Accept(higherIdentifier, DummyCommandValue("2"))
      // when our node sees this
      val handler = new TestAcceptHandler
      val agent = PaxosAgent(1, Follower, initialData, TestHelpers.initialQuorumStrategy )
      val io = new TestIO(stubJournal)
      val PaxosAgent(_, Follower, newData, _) = handler.handleAccept(io, agent, acceptedAccept)
      // it nacks
      io.sent().headOption.value match {
        case MessageAndTimestamp(ack: AcceptNack, _) => // good
        case x => fail(x.toString)
      }
      // and does not update any state
      newData shouldBe initialData
    }

    def `sould nack and accept below current promise`(): Unit = {
      val id = Identifier(0, BallotNumber(0, 0), 0)
      val accept = Accept(id, NoOperationCommandValue)
      val handler = new TestAcceptHandler
      val journal = new UndefinedJournal
      val agent = PaxosAgent(1, Follower, TestHelpers.initialData, TestHelpers.initialQuorumStrategy)
      val io = new TestIO(journal)
      handler.handleAccept(io, agent, accept)
      io.sent().headOption.value match {
        case MessageAndTimestamp(ack: AcceptNack, _) => // good
        case x => fail(x.toString)
      }
    }
  }

}
