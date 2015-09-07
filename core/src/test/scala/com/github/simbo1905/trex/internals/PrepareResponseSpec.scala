package com.github.simbo1905.trex.internals

import akka.actor.{ActorRef, ActorSystem}
import akka.event.LoggingAdapter
import akka.testkit.{TestProbe, TestKit}
import com.github.simbo1905.trex.Journal
import com.github.simbo1905.trex.internals.AllStateSpec._
import org.scalatest.{Matchers, WordSpecLike}

import scala.collection.SortedMap
import scala.collection.mutable.ArrayBuffer
import Ordering._

class TestPrepareResponseHandler extends PrepareResponseHandler {
  val broadcastValues: ArrayBuffer[Any] = ArrayBuffer()

  override def backdownData(data: PaxosData): PaxosData = PaxosActor.backdownData(data, randomTimeout)

  override def log: LoggingAdapter = NoopLoggingAdapter

  override def requestRetransmissionIfBehind(data: PaxosData, sender: ActorRef, from: Int, highestCommitted: Identifier): Unit = {}

  override def randomTimeout: Long = 1234L

  override def broadcast(msg: Any): Unit = broadcastValues += msg

  override def journal: Journal = ???
}

class PrepareResponseSpec extends TestKit(ActorSystem("PrepareResponseSpec")) with WordSpecLike with Matchers {

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
    "if we get a majority negative response does not boardcast messages and backs down" in {
      // given
      val handler = new TestPrepareResponseHandler{
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
