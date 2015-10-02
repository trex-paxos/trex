package com.github.simbo1905.trex.library

import org.scalatest.{Matchers, OptionValues, WordSpecLike}

class TestableCommitHandler extends CommitHandler[DummyRemoteRef] with OptionValues

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

class CommitHandlerTests extends WordSpecLike with Matchers with OptionValues {
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
      val (newProgress, results) = handler.commit(new TestIO(stubJournal){
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
  }
}
