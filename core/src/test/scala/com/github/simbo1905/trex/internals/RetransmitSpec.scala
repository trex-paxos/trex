package com.github.simbo1905.trex.internals

import akka.actor.{ActorSystem, ActorRef}
import akka.event.LoggingAdapter
import akka.testkit.{TestProbe, TestKit}
import com.github.simbo1905.trex.{JournalBounds, Journal}
import com.github.simbo1905.trex.internals.RetransmitHandler.{ResponseState, AcceptState, CommitState}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpecLike}

import scala.collection.generic.SeqFactory

class UndefinedRetransmitHandler extends RetransmitHandler {
  override def log: LoggingAdapter = NoopLoggingAdapter

  override def nodeUniqueId: Int = 0

  override def deliver(value: CommandValue): Any = ???

  override def journal: Journal = ???

  override def processRetransmitResponse(response: RetransmitResponse, nodeData: PaxosData): Retransmission = ???

  def send(actor: ActorRef, msg: Any): Unit = ???
}

class UndefinedJournal extends Journal {
  override def save(progress: Progress): Unit = ???

  override def bounds: JournalBounds = ???

  override def load(): Progress = ???

  override def accepted(logIndex: Long): Option[Accept] = ???

  override def accept(a: Accept*): Unit = ???
}

object RetransmitSpec {
  // given some retransmitted committed values
  val v1 = ClientRequestCommandValue(0, Array[Byte](0))
  val v3 = ClientRequestCommandValue(2, Array[Byte](2))

  val identifier98: Identifier = Identifier(1, BallotNumber(1, 1), 98L)
  val identifier99: Identifier = Identifier(2, BallotNumber(2, 2), 99L)
  val identifier100: Identifier = Identifier(3, BallotNumber(3, 3), 100L)
  val identifier101: Identifier = Identifier(3, BallotNumber(3, 3), 101L)

  val a98 = Accept(identifier98, v1)
  val a99 = Accept(identifier99, NoOperationCommandValue)
  val a100 = Accept(identifier100, v3)
  val a101 = Accept(identifier101, v3)

  val accepts98thru100 = Seq(a98, a99, a100)
  val misorderedAccepts = Seq(a98, a99, a101, a100)

  def journaled98thru100(logIndex: Long): Option[Accept] = {
    logIndex match {
      case 98L => Option(a98)
      case 99L => Option(a99)
      case 100L => Option(a100)
      case _ => None
    }
  }

  def journaled98thru101(logIndex: Long): Option[Accept] = {
    logIndex match {
      case 98L => Option(a98)
      case 99L => Option(a99)
      case 100L => Option(a100)
      case 101L => Option(a101)
      case _ => None
    }
  }

  val lowValue = Int.MinValue + 1
  val initialDataCommittedSlotOne = PaxosData(
    Progress(
      BallotNumber(lowValue, lowValue), Identifier(0, BallotNumber(lowValue, lowValue), 1)
    ), 0, 0, 3)
}

class RetransmitSpec extends TestKit(ActorSystem("RetransmitSpec", AllStateSpec.config))
with WordSpecLike
with Matchers
with MockFactory {

  import RetransmitSpec._

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
      val sender: ActorRef = TestProbe().ref
      var sent: Option[Any] = None
      val handler = new UndefinedRetransmitHandler {

        override def journal: Journal = stubJournal

        override def send(actor: ActorRef, msg: Any): Unit = {
          sent = Some(msg)
          actor shouldBe sender
        }
      }
      // when we send it a request and we have only uncommitted values
      handler.handleRetransmitRequest(sender, RetransmitRequest(2, 0, 0L), initialDataCommittedSlotOne)
      // then
      val expected = Some(RetransmitResponse(0, 2, Seq(a98), Seq(a99)))
      sent match {
        case `expected` => // good
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
      val handler = new UndefinedRetransmitHandler {
        override def processRetransmitResponse(response: RetransmitResponse, nodeData: PaxosData): Retransmission = Retransmission(progress, accepts98thru100, accepts98thru100.map(_.value))

        override def journal: Journal = stubJournal

        override def deliver(value: CommandValue): Any = {
          deliveredWithTs = deliveredWithTs :+(System.nanoTime(), value)
        }

        override def send(actor: ActorRef, msg: Any): Unit = {}
      }
      // when it is passed a retransmit response
      handler.handleRetransmitResponse(RetransmitResponse(1, 0, accepts98thru100, Seq.empty), AllStateSpec.initialData)
      // then we deliver before we save
      deliveredWithTs.headOption.getOrElse(fail("empty delivered list")) match {
        case (ts, _) =>
          assert(saveTs != 0 && ts != 0 && ts < saveTs)
      }
      // and we saved before we accepted
      assert(saveTs != 0 && acceptTs != 0 && saveTs < acceptTs)
      // and we filtered out NoOp values
      assert(deliveredWithTs.size == 2)
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
