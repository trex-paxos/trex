package com.github.simbo1905.trex.internals

import akka.actor.{ActorRef, ActorSystem}
import akka.event.LoggingAdapter
import akka.testkit.{TestProbe, TestKit}
import com.github.simbo1905.trex.Journal
import com.github.simbo1905.trex.internals.AllStateSpec._
import org.scalatest.{OptionValues, Matchers, WordSpecLike}

import scala.collection.SortedMap
import scala.collection.mutable.ArrayBuffer
import Ordering._

class TestPrepareResponseHandler extends PrepareResponseHandler {
  val broadcastValues: ArrayBuffer[(Any,Long)] = ArrayBuffer()

  override def backdownData(data: PaxosData): PaxosData = PaxosActor.backdownData(data, randomTimeout)

  override def log: LoggingAdapter = NoopLoggingAdapter

  override def requestRetransmissionIfBehind(data: PaxosData, sender: ActorRef, from: Int, highestCommitted: Identifier): Unit = {}

  override def randomTimeout: Long = 1234L

  override def broadcast(msg: Any): Unit = {
    val update = (msg, System.nanoTime())
    broadcastValues += update
  }

  override def journal: Journal = ???
}

class PrepareResponseSpec extends TestKit(ActorSystem("PrepareResponseSpec")) with WordSpecLike with Matchers with OptionValues {

  import PrepareResponseSpec._

  "PrepareResponseHandler" should {
    "ignore a response that it is not awaiting" in {
      // given
      val handler = new TestPrepareResponseHandler
      val vote = PrepareAck(Identifier(1, BallotNumber(2, 3), 4L), 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), 12, 13, None)
      val initialData = AllStateSpec.initialData
      // when
      val (role, state) = handler.handlePrepareResponse(0, Recoverer, TestProbe().ref, vote, initialData)
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
      val handler = new TestPrepareResponseHandler {
        override def journal: Journal = AllStateSpec.tempRecordTimesFileJournal
      }
      val vote = PrepareNack(recoverHighPrepare.id, 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), 12, 13)
      // when
      val (role, _) = handler.handlePrepareResponse(0, Recoverer, TestProbe().ref, vote, selfNackPrepares)
      // then we are a follower
      role match {
        case Follower => // good
        case x => fail(x.toString)
      }
      // and we sent no messages
      handler.broadcastValues.size shouldBe 0
    }
    "issues more prepares and self acks if other nodes show higher accepted index" in {
      // given
      val handler = new TestPrepareResponseHandler {
        override def journal: Journal = AllStateSpec.tempRecordTimesFileJournal
      }
      val otherAcceptedIndex = 2L // recoverHighPrepare.id.logIndex + 1
      val vote = PrepareAck(recoverHighPrepare.id, 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), otherAcceptedIndex, 13, None)
      // when
      val (role, data) = handler.handlePrepareResponse(0, Recoverer, TestProbe().ref, vote, selfAckPrepares)
      // then we are still a recoverer as not finished with all prepares
      role match {
        case Recoverer => // good
        case x => fail(x.toString)
      }
      // and we send on prepare and one accept
      handler.broadcastValues match {
        case ArrayBuffer((p: Prepare, _), (a: Accept, _)) =>
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
      data.prepareResponses.headOption.get match {
        case (id, map) if id.logIndex == 2 => map.get(0) match {
          case Some(r: PrepareAck) => // good
          case f => fail(f.toString)
        }
        case f => fail(f.toString)
      }
    }
    "issues more prepares and self nacks if other nodes show higher accepted index and has given higher promise" in {
      // given
      val handler = new TestPrepareResponseHandler {
        override def journal: Journal = AllStateSpec.tempRecordTimesFileJournal
      }
      val otherAcceptedIndex = 2L // recoverHighPrepare.id.logIndex + 1
      val vote = PrepareAck(recoverHighPrepare.id, 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), otherAcceptedIndex, 13, None)
      // when
      val higherPromise = Progress.highestPromisedLens.set(selfAckPrepares.progress, BallotNumber(Int.MaxValue, Int.MaxValue))
      val (role, data) = handler.handlePrepareResponse(0, Recoverer, TestProbe().ref, vote, selfAckPrepares.copy(progress = higherPromise))
      // then we are still a recoverer as not finished with all prepares
      role match {
        case Recoverer => // good
        case x => fail(x.toString)
      }
      // and we send on prepare and one accept
      handler.broadcastValues match {
        case ArrayBuffer(pAndTime, aAndTime) =>
          pAndTime._1 match {
            case p: Prepare if p.id.logIndex == otherAcceptedIndex => // good
            case f => fail(f.toString)
          }
          aAndTime._1 match {
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
      val tempJournal = AllStateSpec.tempRecordTimesFileJournal
      val handler = new TestPrepareResponseHandler {
        override def journal: Journal = tempJournal
      }
      // and an ack vote showing no higher accepted log index
      val vote = PrepareAck(recoverHighPrepare.id, 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), recoverHighPrepare.id.logIndex, 13, None)
      // when
      val (role, data) = handler.handlePrepareResponse(0, Recoverer, TestProbe().ref, vote, selfAckPrepares)
      // then we promote to leader
      role match {
        case Leader => // good
        case x => fail(x.toString)
      }
      // and we send one accept
      handler.broadcastValues match {
        case ArrayBuffer((a: Accept, _)) =>
          a match {
            case a if a.id.logIndex == 1L => // good
            case f => fail(f.toString)
          }
        case f => fail(f.toString)
      }
      // and we have the self vote for the accept
      data.acceptResponses.headOption.getOrElse(fail) match {
        case (id,AcceptResponsesAndTimeout(_, a: Accept, responses)) if id == recoverHighPrepare.id && a.id == id=>
          responses.headOption.getOrElse(fail) match {
            case (0, r: AcceptAck) => // good
            case f => fail(f.toString)
          }
        case f => fail(f.toString)
      }
      // and we have journalled the accept at a time before sent
      val sendTime = handler.broadcastValues.headOption.get match {
        case (a: Accept, ts: Long) => ts
        case f => fail(f.toString)
      }
      tempJournal.actionsWithTimestamp.toMap.getOrElse("accept", fail) match {
        case TimeAndParameter(saveTime, Seq(a: Accept)) if a.id.logIndex == 1 && saveTime < sendTime => // good
        case f => fail(f.toString)
      }
    }
    "issues an accept which it does not journal if it has not made a higher promise" in {
      // given a handler
      val tempJournal = AllStateSpec.tempRecordTimesFileJournal
      val handler = new TestPrepareResponseHandler {
        override def journal: Journal = tempJournal
      }
      // and an ack vote showing no higher accepted log index
      val vote = PrepareAck(recoverHighPrepare.id, 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), recoverHighPrepare.id.logIndex, 13, None)
      // when
      val higherPromise = Progress.highestPromisedLens.set(selfAckPrepares.progress, BallotNumber(Int.MaxValue, Int.MaxValue))
      val (role, data) = handler.handlePrepareResponse(0, Recoverer, TestProbe().ref, vote, selfAckPrepares.copy(progress = higherPromise))
      // then we promote to leader
      role match {
        case Leader => // good
        case x => fail(x.toString)
      }
      // and we send one accept
      handler.broadcastValues match {
        case ArrayBuffer((a: Accept, _)) =>
          a match {
            case a if a.id.logIndex == 1L => // good
            case f => fail(f.toString)
          }
        case f => fail(f.toString)
      }
      // and we have the self vote for the accept
      data.acceptResponses.headOption.getOrElse(fail) match {
        case (id,AcceptResponsesAndTimeout(_, a: Accept, responses)) if id == recoverHighPrepare.id && a.id == id=>
          responses.headOption.getOrElse(fail) match {
            case (0, r: AcceptNack) => // good
            case f => fail(f.toString)
          }
        case f => fail(f.toString)
      }
      // and we have not journalled the accept
      tempJournal.actionsWithTimestamp.size shouldBe 0
    }
  }
}

object PrepareResponseSpec {
  val recoverHighPrepare = Prepare(Identifier(0, BallotNumber(lowValue + 1, 0), 1L))
  val highPrepareEpoch = Some(recoverHighPrepare.id.number)
  val prepareSelfAck = SortedMap.empty[Identifier, Map[Int, PrepareResponse]] ++
    Seq((recoverHighPrepare.id -> Map(0 -> PrepareAck(recoverHighPrepare.id, 0, initialData.progress, 0, 0, None))))
  val selfAckPrepares = initialData.copy(clusterSize = 3, epoch = highPrepareEpoch, prepareResponses = prepareSelfAck, acceptResponses = SortedMap.empty)
  val prepareSelfNack = SortedMap.empty[Identifier, Map[Int, PrepareResponse]] ++
    Seq((recoverHighPrepare.id -> Map(0 -> PrepareNack(recoverHighPrepare.id, 0, initialData.progress, 0, 0))))
  val selfNackPrepares = initialData.copy(clusterSize = 3, epoch = highPrepareEpoch, prepareResponses = prepareSelfNack, acceptResponses = SortedMap.empty)

}
