package com.github.simbo1905.trex.library

import org.scalatest.{Matchers, OptionValues, WordSpecLike}

import scala.collection.mutable.ArrayBuffer

case class TimeAndMessage(message: Any, time: Long)

class TestPrepareResponseHandlerNoRetransmission extends PrepareResponseHandler[TestClient] with BackdownData[TestClient] {
  override def requestRetransmissionIfBehind(io: PaxosIO[TestClient], agent: PaxosAgent[TestClient], from: Int, highestCommitted: Identifier): Unit = {}
}

class PrepareResponseSpec extends WordSpecLike with Matchers with OptionValues {

  import TestHelpers._

  "PrepareResponseHandler" should {
    "ignore a response that it is not awaiting" in {
      // given
      val handler = new TestPrepareResponseHandlerNoRetransmission
      val vote = PrepareAck(Identifier(1, BallotNumber(2, 3), 4L), 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), 12, 13, None)
      val PaxosAgent(_, role, state) = handler.handlePrepareResponse(new TestIO(new UndefinedJournal) {
        override def randomTimeout: Long = 1234L
      }, PaxosAgent(0, Recoverer, initialData), vote)
      // then
      role match {
        case Recoverer => // good
        case x => fail(x.toString)
      }
      state match {
        case `initialData` => // good
        case x => fail(x.toString)
      }
    }
    "not boardcast messages and backs down if it gets a majority nack" in {
      // given
      val broadcastValues: ArrayBuffer[TimeAndMessage] = ArrayBuffer()
      val handler = new TestPrepareResponseHandlerNoRetransmission
      val vote = PrepareNack(recoverHighPrepare.id, 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), 12, 13)
      // when
      val PaxosAgent(_, role, _) = handler.handlePrepareResponse(new TestIO(new UndefinedJournal) {
        override def randomTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = {
          val update = TimeAndMessage(msg, System.nanoTime())
          broadcastValues += update
        }
      }, PaxosAgent(0, Recoverer, selfNackPrepares), vote)
      // then we are a follower
      role match {
        case Follower => // good
        case x => fail(x.toString)
      }
      // and we sent no messages
      broadcastValues.size shouldBe 0
    }
    "issues more prepares and self acks if other nodes show higher accepted index" in {
      // given
      val broadcastValues: ArrayBuffer[TimeAndMessage] = ArrayBuffer()
      val handler = new TestPrepareResponseHandlerNoRetransmission
      val otherAcceptedIndex = 2L // recoverHighPrepare.id.logIndex + 1
      val vote = PrepareAck(recoverHighPrepare.id, 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), otherAcceptedIndex, 13, None)
      val emptyJournal = new UndefinedJournal {
        override def accepted(logIndex: Long): Option[Accept] = None

        override def accept(a: Accept*): Unit = {}
      }
      // when
      val PaxosAgent(_, role, data) = handler.handlePrepareResponse(new TestIO(emptyJournal) {
        override def randomTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = {
          val update = TimeAndMessage(msg, System.nanoTime())
          broadcastValues += update
        }
      }, PaxosAgent(0, Recoverer, selfAckPrepares), vote)
      // then we are still a recoverer as not finished with all prepares
      role match {
        case Recoverer => // good
        case x => fail(x.toString)
      }
      // and we send on prepare and one accept
      broadcastValues match {
        case ArrayBuffer(TimeAndMessage(p: Prepare, _), TimeAndMessage(a: Accept, _)) =>
          p match {
            case p if p.id.logIndex == otherAcceptedIndex => // good
            case f => fail(f.toString)
          }
          a match {
            case a if a.id.logIndex == 1L => // good
            case f => fail(f.toString)
          }
        case f => fail(f.toString)
      }
      // and we accepted our own new prepare
      data.prepareResponses.headOption.value match {
        case (id, map) if id.logIndex == 2 => map.get(0) match {
          case Some(r: PrepareAck) => // good
          case f => fail(f.toString)
        }
        case f => fail(f.toString)
      }
    }
    "issues more prepares and self nacks if other nodes show higher accepted index and has given higher promise" in {
      // given
      val broadcastValues: ArrayBuffer[TimeAndMessage] = ArrayBuffer()
      val handler = new TestPrepareResponseHandlerNoRetransmission
      val otherAcceptedIndex = 2L // recoverHighPrepare.id.logIndex + 1
      val vote = PrepareAck(recoverHighPrepare.id, 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), otherAcceptedIndex, 13, None)
      // when
      val higherPromise = Progress.highestPromisedLens.set(selfAckPrepares.progress, BallotNumber(Int.MaxValue, Int.MaxValue))
      val PaxosAgent(_, role, data) = handler.handlePrepareResponse(new TestIO(new UndefinedJournal) {
        override def randomTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = {
          val update = TimeAndMessage(msg, System.nanoTime())
          broadcastValues += update
        }
      }, PaxosAgent(0, Recoverer, selfAckPrepares.copy(progress = higherPromise)), vote)
      // then we are still a recoverer as not finished with all prepares
      role match {
        case Recoverer => // good
        case x => fail(x.toString)
      }
      // and we send on prepare and one accept
      broadcastValues match {
        case ArrayBuffer(pAndTime: TimeAndMessage, aAndTime: TimeAndMessage) =>
          pAndTime.message match {
            case p: Prepare if p.id.logIndex == otherAcceptedIndex => // good
            case f => fail(f.toString)
          }
          aAndTime.message match {
            case a: Accept if a.id.logIndex == 1L => // good
            case f => fail(f.toString)
          }
        case f => fail(f.toString)
      }
      // and we did not accept our own new prepare
      data.prepareResponses.headOption.getOrElse(fail) match {
        case (id, map) if id.logIndex == 2 => map.get(0) match {
          case Some(r: PrepareNack) => // good
          case f => fail(f.toString)
        }
        case f => fail(f.toString)
      }
    }
    "issues an accept which it journals if it has not made a higher promise" in {
      // given a handler that records broadcast time
      val broadcastValues: ArrayBuffer[TimeAndMessage] = ArrayBuffer()
      val handler = new TestPrepareResponseHandlerNoRetransmission
      // and an ack vote showing no higher accepted log index
      val vote = PrepareAck(recoverHighPrepare.id, 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), recoverHighPrepare.id.logIndex, 13, None)
      // and a journal which records save and send time
      var saved: Option[TimeAndParameter] = None
      val journal = new UndefinedJournal {
        override def accept(a: Accept*): Unit = saved = Some(TimeAndParameter(System.nanoTime(), a))
      }
      // and an IO that records send time
      val io = new TestIO(journal) {
        override def randomTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = {
          val update = TimeAndMessage(msg, System.nanoTime())
          broadcastValues += update
        }
      }
      // when
      val PaxosAgent(_, role, data) = handler.handlePrepareResponse(io, PaxosAgent(0, Recoverer, selfAckPrepares), vote)
      // then we promote to leader
      role match {
        case Leader => // good
        case x => fail(x.toString)
      }
      // and we send one accept
      broadcastValues match {
        case ArrayBuffer(TimeAndMessage(a: Accept, _)) =>
          a match {
            case a if a.id.logIndex == 1L => // good
            case f => fail(f.toString)
          }
        case f => fail(f.toString)
      }
      // and we have the self vote for the accept
      data.acceptResponses.headOption.getOrElse(fail) match {
        case (id, AcceptResponsesAndTimeout(_, a: Accept, responses)) if id == recoverHighPrepare.id && a.id == id =>
          responses.headOption.getOrElse(fail) match {
            case (0, r: AcceptAck) => // good
            case f => fail(f.toString)
          }
        case f => fail(f.toString)
      }
      // and we have journalled the accept at a time before sent
      val sendTime = broadcastValues.headOption.value match {
        case TimeAndMessage(a: Accept, ts: Long) => ts
        case f => fail(f.toString)
      }
      assert(saved.value.time < sendTime)
      saved.value.parameter match {
        case Seq(a: Accept) if a.id.logIndex == 1  => // good
        case f => fail(f.toString)
      }
    }
    "issues an accept which it does not journal if it has not made a higher promise" in {
      // given a handler
      val broadcastValues: ArrayBuffer[TimeAndMessage] = ArrayBuffer()
      val handler = new TestPrepareResponseHandlerNoRetransmission
      // and an ack vote showing no higher accepted log index
      val vote = PrepareAck(recoverHighPrepare.id, 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), recoverHighPrepare.id.logIndex, 13, None)
      // when
      val higherPromise = Progress.highestPromisedLens.set(selfAckPrepares.progress, BallotNumber(Int.MaxValue, Int.MaxValue))
      val PaxosAgent(_, role, data) = handler.handlePrepareResponse(new TestIO(new UndefinedJournal) {
        override def randomTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = {
          val update = TimeAndMessage(msg, System.nanoTime())
          broadcastValues += update
        }

      }, PaxosAgent(0, Recoverer, selfAckPrepares.copy(progress = higherPromise)), vote)
      // then we promote to leader
      role match {
        case Leader => // good
        case x => fail(x.toString)
      }
      // and we send one accept
      broadcastValues match {
        case ArrayBuffer(TimeAndMessage(a: Accept, _)) =>
          a match {
            case a if a.id.logIndex == 1L => // good
            case f => fail(f.toString)
          }
        case f => fail(f.toString)
      }
      // and we have the self vote for the accept
      data.acceptResponses.headOption.getOrElse(fail) match {
        case (id, AcceptResponsesAndTimeout(_, a: Accept, responses)) if id == recoverHighPrepare.id && a.id == id =>
          responses.headOption.getOrElse(fail) match {
            case (0, r: AcceptNack) => // good
            case f => fail(f.toString)
          }
        case f => fail(f.toString)
      }
    }
  }
}
