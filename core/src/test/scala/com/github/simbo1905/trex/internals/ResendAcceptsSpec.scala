package com.github.simbo1905.trex.internals

import akka.event.LoggingAdapter
import com.github.simbo1905.trex.Journal
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpecLike}

import scala.collection.SortedMap
import scala.collection.immutable.TreeMap

class TestResendAcceptsHandler extends ResendAcceptsHandler {
  override def highestNumberProgressed(data: PaxosData): BallotNumber = ???

  override def log: LoggingAdapter = NoopLoggingAdapter

  override def nodeUniqueId: Int = 1

  override def randomTimeout: Long = 0L

  override def journal: Journal = ???

  def broadcast(msg: Any): Unit = ???
}

class ResendAcceptsSpec extends WordSpecLike with Matchers with MockFactory {
  import ResendAcceptsSpec._
  "ResendAcceptsHandler" should {
    "find the timed-out accepts in" in {
      val timedout = ResendAcceptsHandler.timedOutResponse(100L, emptyAcceptResponses)
      timedout shouldBe timedOutAt100AcceptResponses
    }
    "detect highest own promise" in {
      val highest = ResendAcceptsHandler.highestPromise(BallotNumber(4, 4), emptyAcceptResponses)
      highest shouldBe BallotNumber(4, 4)
    }
    "detect others highest promise" in {
      val highPromise = emptyAcceptResponses +
        (a99.id -> AcceptResponsesAndTimeout(50L, a99, Map(0 -> AcceptNack(a99.id, 0, progressWith(BallotNumber(99, 99), zeroProgress.highestPromised)))))
      val highest = ResendAcceptsHandler.highestPromise(BallotNumber(1, 1), highPromise)
      highest shouldBe BallotNumber(99, 99)
    }
    "detect others highest committed" in {
      val highPromise = emptyAcceptResponses +
        (a99.id -> AcceptResponsesAndTimeout(50L, a99, Map(0 -> AcceptNack(a99.id, 0, progressWith(zeroProgress.highestPromised, BallotNumber(99, 99))))))
      val highest = ResendAcceptsHandler.highestPromise(BallotNumber(1, 1), highPromise)
      highest shouldBe BallotNumber(99, 99)
    }
    "refresh accepts" in {
      val newNumber = BallotNumber(4, 4)
      val refreshed = ResendAcceptsHandler.refreshAccepts(newNumber, Seq(a98, a99, a100))
      val identifier98: Identifier = Identifier(1, newNumber, 98L)
      val identifier99: Identifier = Identifier(2, newNumber, 99L)
      val identifier100: Identifier = Identifier(3, newNumber, 100L)
      refreshed shouldBe Seq(a98.copy(id = identifier98), a99.copy(id = identifier99), a100.copy(id = identifier100))
    }
    "sets a new timeout per accept on resend with same epoch" in {
      // given
      val handler = new TestResendAcceptsHandler {
        override def randomTimeout = 121L
      }
      // when
      val AcceptsAndData(accepts,data) = handler.computeResendAccepts(Leader, AllStateSpec.initialData.copy(acceptResponses = emptyAcceptResponses), 100L)
      // then
      data.timeout shouldBe 121L
      accepts.size shouldBe 2
      data.acceptResponses.values.map(_.timeout) shouldBe Seq(121L,121L,120L)
    }
    "goes to a one higher epoch on detecting higher promise in responses" in {
      // given
      val handler = new TestResendAcceptsHandler {
        override def randomTimeout: Long = 121L
      }
      val higherPromise = emptyAcceptResponses +
        (a99.id -> AcceptResponsesAndTimeout(50L, a99, Map(0 -> AcceptNack(a99.id, 0, progressWith(zeroProgress.highestPromised, BallotNumber(99, 99))))))
      val newEpoch = BallotNumber(100,1)
      // when
      val AcceptsAndData(accepts,data) = handler.computeResendAccepts(Leader, AllStateSpec.initialData.copy(acceptResponses = higherPromise), 100L)
      // then it move to the 1-higher epoch
      data.epoch match {
        case Some(newEpoch) => // success
        case x => fail(s"$x is not expected BallotNumber(100,100)")
      }
      // has two messages to resend
      accepts.size shouldBe 2
      // then name the new epoch
      accepts.map(_.id.number).foreach {
        _ match {
          case `newEpoch` => // success
          case x => fail(s"$x is not expected BallotNumber(100,100)")
        }
      }
      // top level guard timeout is set
      data.timeout shouldBe 121L
      // the new epoch accepts are listed as the values we are awaiting
      data.acceptResponses.values.map(_.accept).filter(_.id.number == newEpoch) shouldBe accepts
      // we have made to self acks to the two new higher accepts
      data.acceptResponses.values.flatMap(_.responses.values).filter(_.isInstanceOf[AcceptAck]).size shouldBe 2
    }
    "sets a new timeout per accept on resend new epoch" in {
      // given
      val handler = new TestResendAcceptsHandler {
        override def randomTimeout = 121L
      }
      val higherPromise = emptyAcceptResponses +
        (a99.id -> AcceptResponsesAndTimeout(50L, a99, Map(0 -> AcceptNack(a99.id, 0, progressWith(zeroProgress.highestPromised, BallotNumber(99, 99))))))
      // when
      val AcceptsAndData(accepts,data) = handler.computeResendAccepts(Leader, AllStateSpec.initialData.copy(acceptResponses = higherPromise), 100L)
      // then
      data.timeout shouldBe 121L
      accepts.size shouldBe 2
      data.acceptResponses.values.map(_.timeout) shouldBe Seq(121L,121L,120L)
    }
    "journalling and sending happens in the correct order" in {
      // given a journal which records saving and accepting
      val stubJournal = stub[Journal]
      var saveJournalTime = 0L
      var acceptJournalTime = 0L
      stubJournal.save _ when * returns {
        // FIXME broken as this runs immediately
        saveJournalTime = System.nanoTime()
        Unit
      }
      stubJournal.accept _ when * returns {
        // FIXME broken as this runs immediately
        acceptJournalTime = System.nanoTime()
        Unit
      }
      // and a handler that records broadcase time
      var sendTime = 0L
      val handler = new TestResendAcceptsHandler {
        override def randomTimeout = 121L
        override def journal: Journal = stubJournal
        override def broadcast(msg: Any): Unit = sendTime = System.nanoTime()
      }
      // when we get it to do work
      handler.handleResendAccepts(Leader, AllStateSpec.initialData.copy(acceptResponses = emptyAcceptResponses), 100L)
      // then we saved, accepted and sent
      assert(saveJournalTime > 0 && acceptJournalTime > 0 && sendTime > 0)
      // in the correct order had we done a full round of paxos which is promise, accept then send last
      assert(sendTime > acceptJournalTime && acceptJournalTime > saveJournalTime)
    }
  }
}

object ResendAcceptsSpec {
  import Ordering._
  val identifier98: Identifier = Identifier(1, BallotNumber(1, 1), 98L)
  val identifier99: Identifier = Identifier(2, BallotNumber(2, 2), 99L)
  val identifier100: Identifier = Identifier(3, BallotNumber(3, 3), 100L)

  val a98 = Accept(identifier98, NoOperationCommandValue)
  val a99 = Accept(identifier99, NoOperationCommandValue)
  val a100 = Accept(identifier100, NoOperationCommandValue)

  val emptyAcceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout] = TreeMap(
    a98.id -> AcceptResponsesAndTimeout(100L, a98, Map.empty),
    a99.id -> AcceptResponsesAndTimeout(50L, a99, Map.empty),
    a100.id -> AcceptResponsesAndTimeout(120L, a100, Map.empty)
  )

  val timedOutAt100AcceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout] = TreeMap(
    a98.id -> AcceptResponsesAndTimeout(100L, a98, Map.empty),
    a99.id -> AcceptResponsesAndTimeout(50L, a99, Map.empty)
  )

  val zeroProgress = Progress(BallotNumber(0,0), Identifier(0, BallotNumber(0,0), 0L))

  def progressWith(promise: BallotNumber, committed: BallotNumber) = zeroProgress.copy(highestPromised = promise,
    highestCommitted = zeroProgress.highestCommitted.copy(number = committed))

}
