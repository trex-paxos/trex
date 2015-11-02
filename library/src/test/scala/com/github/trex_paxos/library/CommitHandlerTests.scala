package com.github.trex_paxos.library

import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OptionValues, WordSpecLike}

import scala.collection.mutable.ArrayBuffer

class TestableCommitHandler extends CommitHandler with OptionValues

object CommitHandlerTests {
  val v1 = ClientRequestCommandValue(0, Array[Byte](0))
  val v3 = ClientRequestCommandValue(2, Array[Byte](2))

  val identifier11: Identifier = Identifier(1, BallotNumber(1, 1), 11L)
  val identifier12: Identifier = Identifier(2, BallotNumber(2, 2), 12L)
  val identifier13: Identifier = Identifier(2, BallotNumber(2, 2), 13L)
  val identifier14: Identifier = Identifier(2, BallotNumber(2, 2), 14L)

  val a11 = Accept(identifier11, v1)
  val a12 = Accept(identifier12, NoOperationCommandValue)
  val a13 = Accept(identifier13, v3)
  val a14 = Accept(identifier14, v3)

  val accepts11thru14 = Seq(a11, a12, a13, a14)

  def journaled11thru14(logIndex: Long): Option[Accept] = {
    logIndex match {
      case 11L => Option(a11)
      case 12L => Option(a12)
      case 13L => Option(a13)
      case 14L => Option(a14)
      case _ => None
    }
  }

}

class CommitHandlerTests extends WordSpecLike with Matchers with MockFactory with OptionValues {

  import CommitHandlerTests._
  import TestHelpers._

  "CommitHandler" should {
    "do nothing if have committed up to the specified log index" in {
      CommitHandler.committableValues(accepts98thru100.lastOption.value.id.number,
        accepts98thru100.lastOption.value.id,
        accepts98thru100.lastOption.value.id.logIndex,
        journaled98thru100) shouldBe Seq.empty[Accept]
    }
    "do nothing if have committed way beyond the specified log index" in {
      CommitHandler.committableValues(accepts98thru100.lastOption.value.id.number,
        accepts98thru100.lastOption.value.id,
        1L,
        journaled98thru100) shouldBe Seq.empty[Accept]
    }
    "do nothing if have a gap in our journal" in {
      CommitHandler.committableValues(accepts98thru100.lastOption.value.id.number,
        accepts98thru100.lastOption.value.id,
        999L,
        journaled98thru100) shouldBe Seq.empty[Accept]
    }
    "should not deliver noop values" in {
      // given we have a11 thru a14 in the journal
      val stubJournal: Journal = new UndefinedJournal {
        override def save(progress: Progress): Unit = ()

        override def accepted(logIndex: Long): Option[Accept] = journaled11thru14(logIndex)
      }
      val handler = new TestableCommitHandler
      // and we promised to a12 and have only committed up to a11
      val oldProgress = Progress(a12.id.number, a11.id)
      // when we commit to a14
      val (newProgress, results) = handler.commit(new TestIO(stubJournal) {
        override def deliver(value: CommandValue): Any = value.bytes
      }, PaxosAgent(0, Follower,
        initialData.copy(progress = oldProgress)),
        accepts11thru14.lastOption.value.id
      )
      // then we will have committed
      newProgress.highestCommitted shouldBe accepts11thru14.lastOption.value.id
      results.size shouldBe 3
      val resultsMap = results.toMap
      resultsMap.get(a12.id) shouldBe Some(NoOperationCommandValue.bytes)
      resultsMap.get(a13.id) shouldBe Some(a13.value.bytes)
      resultsMap.get(a14.id) shouldBe Some(a14.value.bytes)
    }
    "do nothing if no committable values" in {
      // given a handler
      val handler = new Object with CommitHandler
      // and an empty journal
      val emptyJournal = stub[Journal]
      (emptyJournal.accepted _) when (*) returns None
      // and an agent
      val agent = PaxosAgent(0, Follower, initialData)
      // when
      val (newProgress, result) = handler.commit(new UndefinedIO with SilentLogging {
        override def journal: Journal = emptyJournal
      }, agent, a98.id)
      // then
      assert(newProgress == agent.data.progress)
      assert(result.isEmpty)
    }
    "cancels prepare work and sets a new timeout if it sees commit with higher heartbeat" in {
      // given a handler
      val handler = new Object with CommitHandler
      // and an agent
      val agent = PaxosAgent(0, Follower, initialData.copy(prepareResponses = prepareSelfAck, leaderHeartbeat = Long.MinValue))
      // and an io with a new timeout
      val io = new UndefinedIO {
        override def randomTimeout: Long = Long.MaxValue
      }
      // and a commit with a higher heartbeat
      val commitWithHigherHeartbeat = Commit(initialData.progress.highestCommitted, Long.MaxValue)
      // when
      val PaxosAgent(_, role, data) = handler.handleFollowerCommit(io, agent, commitWithHigherHeartbeat)
      // then
      data.timeout shouldBe Long.MaxValue
      data.prepareResponses.isEmpty shouldBe true
      data.leaderHeartbeat shouldBe Long.MaxValue
    }
    "cancels prepare work and sets a new timeout if it sees commit with higher committed number" in {
      // given a handler
      val handler = new Object with CommitHandler
      // some number
      val high = BallotNumber(Int.MaxValue, Int.MaxValue)
      // and an agent
      val agent = PaxosAgent(0, Follower, initialData.copy(prepareResponses = prepareSelfAck, leaderHeartbeat = Long.MinValue))
      // and an io with a new timeout
      val io = new UndefinedIO {
        override def randomTimeout: Long = Long.MaxValue
      }
      // and a commit with a higher number
      val commitWithHigherNumber = Commit(initialData.progress.highestCommitted.copy(number = high), Long.MinValue)
      // when
      val PaxosAgent(_, _, data) =
        handler.handleFollowerCommit(io, agent, commitWithHigherNumber)
      // then
      data.timeout shouldBe Long.MaxValue
      data.prepareResponses.isEmpty shouldBe true
      data.leaderHeartbeat shouldBe Long.MinValue
    }
    "sends a retransmit if it has no committable value in the journal" in {
      // given a handler
      val handler = new Object with CommitHandler
      // and an empty journal
      val emptyJournal = stub[Journal]
      (emptyJournal.accepted _) when (*) returns None
      // and an agent
      val agent = PaxosAgent(0, Follower, initialData)
      // when
      val sent: ArrayBuffer[PaxosMessage] = ArrayBuffer()
      val PaxosAgent(_, _, data) = handler.handleFollowerCommit(new UndefinedIO with SilentLogging {
        override def journal: Journal = emptyJournal

        override def randomTimeout: Long = 1234L

        override def send(msg: PaxosMessage) = sent += msg
      }, agent, Commit(a98.id))
      // then
      data.progress shouldBe initialData.progress
      sent.headOption.value shouldBe RetransmitRequest(0, a98.id.from, initialData.progress.highestCommitted.logIndex)
    }
    "ignore repeated commit" in {
      // given a handler
      val handler = new Object with CommitHandler
      // and an empty journal
      val emptyJournal = stub[Journal]
      (emptyJournal.accepted _) when (*) returns None
      // and an agent
      val agent = PaxosAgent(0, Follower, initialData)
      // when
      val PaxosAgent(_, _, data) = handler.handleFollowerCommit(new UndefinedIO with SilentLogging,
        agent, Commit(initialData.progress.highestCommitted, initialData.leaderHeartbeat))
      // then
      data shouldBe initialData
    }
  }
}
