package com.github.trex_paxos.internals

import akka.actor.ActorRef
import akka.testkit.{TestActorRef, TestKit}
import com.github.trex_paxos.library._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.postfixOps

trait LeaderLikeSpec {
  self: TestKit with MockFactory with Matchers with PaxosLenses =>

  import AllStateSpec._
  import Ordering._
  import PaxosActor.Configuration

  def resendsSameAcceptOnTimeoutNoOtherInfo(role: PaxosRole)(implicit sender: ActorRef): Unit = {
    require(role == Leader || role == Recoverer)
    val stubJournal: Journal = stub[Journal]

    val lastCommitted = Identifier(0, BallotNumber(1, 0), 98L)
    // self voted on accept id99
    val id99 = Identifier(0, BallotNumber(1, 0), 99L)
    val a99 = Accept(id99, ClientRequestCommandValue(0, Array[Byte](1, 1)))
    val votes = TreeMap(id99 -> AcceptResponsesAndTimeout(0L, a99, Map(0 -> AcceptAck(id99, 0, initialData.progress))))
    val responses = acceptResponsesLens.set(initialData, votes)
    val oldProgress = Progress.highestPromisedHighestCommitted.set(responses.progress, (lastCommitted.number, lastCommitted))
    val timenow = 999L
    val fsm = TestActorRef(new TestPaxosActor(Configuration(config, 3), 0, sender, stubJournal, ArrayBuffer.empty, None) {
      override def clock() = timenow
    })
    fsm.underlyingActor.setAgent(role, responses.copy(epoch = Some(BallotNumber(1, 0)), progress = oldProgress))

    // when it gets a timeout
    fsm ! CheckTimeout

    // it reboardcasts the accept
    expectMsg(100 millisecond, a99)

    // and sets a fresh timeout
    assert(fsm.underlyingActor.data.timeout > 0 && fsm.underlyingActor.data.timeout - timenow < config.getLong(PaxosActor.leaderTimeoutMaxKey))
  }

  // TODO these next few tests need to be DRYed
  def resendsHigherAcceptOnLearningOtherNodeHigherPromise(role: PaxosRole)(implicit sender: ActorRef): Unit = {
    require(role == Leader || role == Recoverer)
    val tempJournal = AllStateSpec.tempRecordTimesFileJournal

    val lastCommitted = Identifier(0, BallotNumber(1, 0), 98L)
    // given a leader who has boardcast slot 99 and seen a nack
    val id99 = Identifier(0, BallotNumber(1, 0), 99L)
    val a99 = Accept(id99, ClientRequestCommandValue(0, Array[Byte](1, 1)))
    val votes = TreeMap(id99 -> AcceptResponsesAndTimeout(0L, a99, Map(
      0 -> AcceptAck(id99, 0, initialData.progress),
      1 -> AcceptNack(id99, 1, initialData.progress.copy(highestPromised = BallotNumber(22, 2)))
    )))
    val responses = acceptResponsesLens.set(initialData, votes)
    val committed = Progress.highestPromisedHighestCommitted.set(responses.progress, (lastCommitted.number, lastCommitted))
    val timenow = 999L
    var sendTime = 0L
    val fsm = TestActorRef(new TestPaxosActor(Configuration(config, 3), 0, sender, tempJournal, ArrayBuffer.empty, None) {
      override def clock() = timenow
      override def broadcast(msg: PaxosMessage): Unit = {
        sendTime = System.nanoTime()
        super.broadcast(msg)
      }
    })
    fsm.underlyingActor.setAgent(role, responses.copy(progress = committed, epoch = Some(BallotNumber(1, 0))))

    // when it gets a timeout
    fsm ! CheckTimeout

    // it boardcasts a higher accept
    val newEpoch = BallotNumber(23, 0)
    val newIdentifier = Identifier(0, newEpoch, 99L)
    expectMsgPF(100 millisecond) {
      case a: Accept if a.id == newIdentifier && a.value == a99.value =>
      case a: Accept =>
        fail(s"${a.id}, ${newIdentifier} => ${a.id == newIdentifier} || ${a.value}, ${a99.value} => ${a.value == a99.value}")
      case x => fail(x.toString)
    }
    // and sets its
    assert(fsm.underlyingActor.data.epoch == Some(newEpoch))
    // and sets a fresh timeout
    assert(fsm.underlyingActor.data.timeout > 0 && fsm.underlyingActor.data.timeout - timenow < config.getLong(PaxosActor.leaderTimeoutMaxKey))

    // and it sent out the messages only after having journalled its own promise
    val saveTime = tempJournal.actionsWithTimestamp.toMap.getOrElse("save", fail).time
    assert(saveTime != 0 && sendTime != 0 && saveTime < sendTime)
  }

  def resendsHigherAcceptOnHavingMadeAHigherPromiseAtTimeout(role: PaxosRole)(implicit sender: ActorRef): Unit = {
    require(role == Leader || role == Recoverer)
    val stubJournal: Journal = stub[Journal]

    val lastCommitted = Identifier(0, BallotNumber(1, 0), 98L)
    // given a leader who has boardcast slot 99 and seen a nack
    val id99 = Identifier(0, BallotNumber(1, 0), 99L)
    val a99 = Accept(id99, ClientRequestCommandValue(0, Array[Byte](1, 1)))
    val votes = TreeMap(id99 -> AcceptResponsesAndTimeout(0L, a99, Map(
      0 -> AcceptAck(id99, 0, initialData.progress)
    )))
    val responses = acceptResponsesLens.set(initialData, votes)
    val committed = Progress.highestPromisedHighestCommitted.set(responses.progress, (BallotNumber(22, 2), lastCommitted))
    val timenow = 999L
    val fsm = TestActorRef(new TestPaxosActor(Configuration(config, 3), 0, sender, stubJournal, ArrayBuffer.empty, None) {
      override def clock() = timenow
      override def freshTimeout(interval: Long): Long = 1234L
    })
    fsm.underlyingActor.setAgent(role, responses.copy(progress = committed, epoch = Some(BallotNumber(1, 0))))

    // when it gets a timeout
    fsm ! CheckTimeout

    // it boardcasts a higher accept
    val newEpoch = BallotNumber(23, 0)
    val newIdentifier = Identifier(0, newEpoch, 99L)
    expectMsgPF(100 millisecond) {
      case a: Accept if a.id == newIdentifier && a.value == a99.value =>
      case x => fail(x.toString)
    }

    // and sets its
    assert(fsm.underlyingActor.data.epoch == Some(newEpoch))
    // and sets a fresh timeout
    fsm.underlyingActor.data.timeout shouldBe 1234L
  }

}