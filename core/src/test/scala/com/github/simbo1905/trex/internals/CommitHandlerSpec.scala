package com.github.simbo1905.trex.internals

import akka.event.LoggingAdapter
import com.github.simbo1905.trex.Journal
import org.scalatest.{Matchers, WordSpecLike}

class TestableCommitHandler extends CommitHandler {
  override def log: LoggingAdapter = NoopLoggingAdapter

  override def nodeUniqueId: Int = 0

  override def trace(state: PaxosRole, data: PaxosData, payload: CommandValue): Unit = {}

  override def deliver(value: CommandValue): Any = {}

  override def journal: Journal = ???
}

object CommitHandlerSpec {
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

class CommitHandlerSpec extends WordSpecLike with Matchers {
  import RetransmitSpec._
  import CommitHandlerSpec._
  "CommitHandler" should {
    "do nothing if have committed up to the specified log index" in {
      CommitHandler.committableValues(accepts98thru100.last.id.number,
        accepts98thru100.last.id,
        accepts98thru100.last.id.logIndex,
        journaled98thru100) shouldBe Seq.empty[Accept]
    }
    "do nothing if have committed way beyond the specified log index" in {
      CommitHandler.committableValues(accepts98thru100.last.id.number,
        accepts98thru100.last.id,
        1L,
        journaled98thru100) shouldBe Seq.empty[Accept]
    }
    "do nothing if have a gap in our journal" in {
      CommitHandler.committableValues(accepts98thru100.last.id.number,
        accepts98thru100.last.id,
        999L,
        journaled98thru100) shouldBe Seq.empty[Accept]
    }
    "should not deliver noop values" in {
      // given we have a11 thru a14 in the journal
      var saved: Progress = null
      val stubJournal: Journal = new UndefinedJournal {
        override def save(progress: Progress): Unit = saved = progress
        override def accepted(logIndex: Long): Option[Accept] = journaled11thru14(logIndex)
      }
      val handler = new TestableCommitHandler {
        override def journal: Journal = stubJournal
        override def deliver(value: CommandValue): Any = value.bytes
      }
      // and we promised to a12 and have only committed up to a11
      val oldProgress = Progress(a12.id.number, a11.id)
      // when we commit to a14
      val (newProgress, results) = handler.commit(Follower,
        AllStateSpec.initialData.copy(progress = oldProgress),
        accepts11thru14.last.id,
        oldProgress)
      // then we will have committed
      newProgress.highestCommitted shouldBe accepts11thru14.last.id
      results.size shouldBe 3
      val resultsMap = results.toMap
      resultsMap.get(a12.id) shouldBe Some(NoOperationCommandValue.bytes)
      resultsMap.get(a13.id) shouldBe Some(a13.value.bytes)
      resultsMap.get(a14.id) shouldBe Some(a14.value.bytes)
    }
  }
}
