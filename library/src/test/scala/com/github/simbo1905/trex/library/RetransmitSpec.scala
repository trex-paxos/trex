package com.github.simbo1905.trex.library

import com.github.simbo1905.trex.library.RetransmitHandler.{AcceptState, CommitState, ResponseState}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{OptionValues, Matchers, WordSpecLike}

class TestRetransmitHandler extends RetransmitHandler[TestClient]

class RetransmitSpec extends WordSpecLike
with Matchers
with MockFactory
with OptionValues {

  import TestHelpers._

  "RetransmitHandler request handling" should {
    "return committed accepts in order" in {
      // given 98, 99, 100 are in the journal
      RetransmitHandler.processRetransmitRequest(JournalBounds(98L, 100L), 100L, journaled98thru100, 97L) match {
        case Some(ResponseState(committed, _)) =>
          committed.map(_.id.logIndex) shouldBe Seq(98L, 99L, 100L)
        case None => fail("Expected Some(ResponseState(committed, uncommitted)) but got None")
      }
    }
    "return uncommitted accepts in order" in {
      // given 98, 99, 100 are in the journal
      RetransmitHandler.processRetransmitRequest(JournalBounds(98L, 100L), 97L, journaled98thru100, 97L) match {
        case Some(ResponseState(_, uncommitted)) =>
          uncommitted.map(_.id.logIndex) shouldBe Seq(98L, 99L, 100L)
        case None => fail("Expected Some(ResponseState(committed, uncommitted)) but got None")
      }
    }
    "return None when request falls out of data currently retained in journal" in {
      // given 98, 99, 100 are in the journal
      RetransmitHandler.processRetransmitRequest(JournalBounds(98L, 100L), 97L, journaled98thru100, 10L) match {
        case Some(ResponseState(committed, uncommitted)) =>
          fail(s"Expected None but got Some(ResponseState($committed, $uncommitted))")
        case None => // good
      }
    }
    "return committed and uncommitted values in correct collection" in {
      // given 98 thru 101 are in the journal
      RetransmitHandler.processRetransmitRequest(JournalBounds(98L, 101L), 99L, journaled98thru101, 97L) match {
        case Some(ResponseState(committed, uncommitted)) =>
          committed.map(_.id.logIndex) shouldBe Seq(98L, 99L)
          uncommitted.map(_.id.logIndex) shouldBe Seq(100L, 101L)
        case None => fail("Expected Some(ResponseState(committed, uncommitted)) but got None")
      }
    }
    "sends a response with both committed and uncommitted values" in {
      // given a journal with a value
      val stubJournal = stub[Journal]
      (stubJournal.bounds _) when() returns (JournalBounds(0, 2))
      (stubJournal.accepted _) when (1L) returns Option(a98)
      (stubJournal.accepted _) when (2L) returns Option(a99)
      // and a retransmit handler which records what it sent
      val handler = new TestRetransmitHandler
      // when we send it a request and we have only uncommitted values
      val testIO = new TestIO(new UndefinedJournal){
        override def send(msg: PaxosMessage): Unit = {
          sent = sent :+ MessageAndTimestamp(msg, 0L)
        }

        override def journal: Journal = stubJournal
      }
      handler.handleRetransmitRequest(testIO, PaxosAgent(99, Leader, initialDataCommittedSlotOne), RetransmitRequest(2, 0, 0L))
      // then
      val expected = RetransmitResponse(99, 2, Seq(a98), Seq(a99))
      testIO.sent.headOption.value match {
        case MessageAndTimestamp(msg, 0L) if msg == expected => // good
        case x => fail(s"$x != $expected")
      }
    }
  }

  "RetransmitHandler response handling" should {
    "side effects delivery before saving of promise before journalling accepts" in {
      // given some recognisable processed state
      val progress = Journal.minBookwork
      // and a Journal which records method invocation times
      var saveTs = 0L
      var acceptTs = 0L
      val stubJournal: Journal = new UndefinedJournal {
        override def save(progress: Progress): Unit = {
          saveTs = System.nanoTime()
        }

        override def accept(a: Accept*): Unit = {
          acceptTs = System.nanoTime()
        }
      }
      // and a retransmit handler which records what was delivered when
      var deliveredWithTs: Seq[(Long, CommandValue)] = Seq.empty
      val handler = new TestRetransmitHandler {
        override def processRetransmitResponse(io: PaxosIO[TestClient], agent: PaxosAgent[TestClient], response: RetransmitResponse): Retransmission =
          Retransmission(progress, accepts98thru100, accepts98thru100.map(_.value))

      }
      // when it is passed a retransmit response
      handler.handleRetransmitResponse(new TestIO(new UndefinedJournal){
        override def deliver(value: CommandValue): Any = {
          deliveredWithTs = deliveredWithTs :+(System.nanoTime(), value)
        }

        override def journal: Journal = stubJournal
      }, PaxosAgent[TestClient](99, Follower, initialData), RetransmitResponse(1, 0, accepts98thru100, Seq.empty))
      // then we deliver before we save
      deliveredWithTs.headOption.getOrElse(fail("empty delivered list")) match {
        case (ts, _) =>
          assert(saveTs != 0 && ts != 0 && ts < saveTs)
      }
      // and we saved before we accepted
      assert(saveTs != 0 && acceptTs != 0 && saveTs < acceptTs)
      // and we filtered out NoOp values
      assert(deliveredWithTs.size == 1)
    }

    "commit contiguous values" in {
      // given have committed to just prior to those values
      val identifier97 = Identifier(1, BallotNumber(1, 1), 97L)
      // when
      val CommitState(highestCommitted, committed) = RetransmitHandler.contiguousCommittableCommands(identifier97, accepts98thru100)
      // then
      assert(highestCommitted == accepts98thru100.last.id && committed.size == 3)
    }

    "not commit any values when log index of first accept isn't current log index + 1" in {
      // given have committed to just prior to those values
      val identifier96 = Identifier(1, BallotNumber(1, 1), 96L)
      // when
      val CommitState(highestCommitted, committed) = RetransmitHandler.contiguousCommittableCommands(identifier96, accepts98thru100)
      // then
      assert(highestCommitted == identifier96 && committed.size == 0)
    }

    // misordered messages are a bug on send so the receiver isn't going to reorder them.
    "only commit contiguous values in non-contiguous accept sequence" in {
      val identifier97 = Identifier(1, BallotNumber(1, 1), 97L)
      val CommitState(highestCommitted, committed) = RetransmitHandler.contiguousCommittableCommands(identifier97, misorderedAccepts)
      highestCommitted shouldBe identifier99
      committed.size shouldBe 2
    }

    "accept and compute promise for messages above or equal to current promise" in {
      val currentPromise = accepts98thru100.head.id.number
      val AcceptState(highest, acceptable) = RetransmitHandler.acceptableAndPromiseNumber(currentPromise, accepts98thru100)
      highest should be(accepts98thru100.last.id.number)
      acceptable should be(accepts98thru100)
    }

    "not accept messages below current promise" in {
      val currentPromise = accepts98thru100.last.id.number
      val AcceptState(highest, acceptable) = RetransmitHandler.acceptableAndPromiseNumber(currentPromise, accepts98thru100)
      highest should be(accepts98thru100.last.id.number)
      acceptable should be(Seq(accepts98thru100.last))
    }

  }
}
