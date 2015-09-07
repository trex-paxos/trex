package com.github.simbo1905.trex.internals

import akka.actor.{ActorRef, ActorSystem}
import akka.event.LoggingAdapter
import akka.testkit.{TestProbe, TestKit}
import com.github.simbo1905.trex.Journal
import org.scalatest.{Matchers, WordSpecLike}

class TestPrepareResponseHandler extends PrepareResponseHandler {
  override def backdownData(data: PaxosData): PaxosData = data

  override def log: LoggingAdapter = NoopLoggingAdapter

  override def requestRetransmissionIfBehind(data: PaxosData, sender: ActorRef, from: Int, highestCommitted: Identifier): Unit = {}

  override def randomTimeout: Long = 1234L

  override def broadcast(msg: Any): Unit = {}

  override def journal: Journal = ???
}

class PrepareResponseSpec extends TestKit(ActorSystem("PrepareResponseSpec")) with WordSpecLike with Matchers {
  "PrepareResponseHandler" should {
    "ignore a response that it is not awaiting" in {
      // given
      val handler = new TestPrepareResponseHandler
      val vote = PrepareAck(Identifier(1, BallotNumber(2, 3), 4L), 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), 12, 13, None)
      val initialData = AllStateSpec.initialData
      // when
      val (role, state) = handler.handlePrepareResponse(0, Follower, TestProbe().ref, vote, initialData)
      // then
      role match {
        case Follower => // good
        case x => fail(x.toString)
      }
      state match {
        case `initialData` => // good
        case x => fail(x.toString)
      }
    }
  }
}
