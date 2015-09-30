package com.github.simbo1905.trex.internals

import akka.actor.ActorRef
import akka.testkit.{TestActorRef, TestKit}
import com.github.simbo1905.trex._
import com.github.simbo1905.trex.library._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.postfixOps

trait LeaderLikeSpec {
  self: TestKit with MockFactory with AllStateSpec with Matchers with PaxosLenses[ActorRef] =>

  import AllStateSpec._
  import Ordering._
  import PaxosActor.Configuration

  def ignoreCommitMessageLogIndexLessThanLastCommit(role: PaxosRole)(implicit sender: ActorRef) {
    require(role == Leader || role == Recoverer)
    val stubJournal: Journal = stub[Journal]
    // given a leaderlike with no responses
    val identifier = Identifier(0, BallotNumber(lowValue + 1, 0), 1L)
    val lessThan = Identifier(0, BallotNumber(lowValue, 0), 0L)
    val data = initialData.copy(clusterSize = 3, progress = initialData.progress.copy(highestCommitted = identifier))
    val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, stubJournal, ArrayBuffer.empty, None))
    fsm.underlyingActor.setAgent(role, data)
    // when it gets commit
    val commit = Commit(lessThan)
    fsm ! commit
    // it sends no messages
    expectNoMsg(25 millisecond)
    // and stays a recoverer
    assert(fsm.underlyingActor.role == role)
  }

  def ignoreCommitMessageSameSlotLowerNodeIdentifier(role: PaxosRole)(implicit sender: ActorRef) {
    require(role == Leader || role == Recoverer)
    val stubJournal: Journal = stub[Journal]
    // given
    val node2slot1Identifier = Identifier(2, BallotNumber(lowValue + 1, 2), 1L)
    val node2 = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 2, sender, stubJournal, ArrayBuffer.empty, None))
    node2.underlyingActor.setAgent(role, initialData.copy(epoch = Some(node2slot1Identifier.number), clusterSize = 3, progress = initialData.progress.copy(highestCommitted = node2slot1Identifier)))
    // when node2 it gets commit for same slot but lower nodeIdentifier
    val node0slot1Identifier = Identifier(0, BallotNumber(lowValue + 1, 0), 1L)
    val commit = Commit(node0slot1Identifier)
    node2 ! commit
    // it sends no messages
    expectNoMsg(25 millisecond)
    // and stays a recoverer
    assert(node2.underlyingActor.role == role)
  }

  def backdownToFollowerOnCommitSameSlotHigherNodeIdentifier(role: PaxosRole)(implicit sender: ActorRef) {
    require(role == Leader || role == Recoverer)
    val stubJournal: Journal = stub[Journal]
    // given
    val node2slot1Identifier = Identifier(2, BallotNumber(lowValue + 1, 2), 1L)
    val timenow = 999L
    val node2 = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 2, sender, stubJournal, ArrayBuffer.empty, None){
      override def clock() = timenow
    })
    node2.underlyingActor.setAgent(role, initialData.copy(epoch = Some(node2slot1Identifier.number), clusterSize = 3, progress = initialData.progress.copy(highestCommitted = node2slot1Identifier)))
    // when node2 it gets commit for same slot but lower nodeIdentifier
    val node3slot1Identifier = Identifier(0, BallotNumber(lowValue + 1, 3), 1L)
    val commit = Commit(node3slot1Identifier)
    node2 ! commit
    // it sends no messages
    expectNoMsg(25 millisecond)
    // and returns to be follower
    assert(node2.underlyingActor.role == Follower)
    // and clears data
    assert(node2.underlyingActor.data.acceptResponses.isEmpty)
    assert(node2.underlyingActor.data.prepareResponses.isEmpty)
    assert(node2.underlyingActor.data.epoch == None)
    // and sets a fresh timeout
    assert(node2.underlyingActor.data.timeout > 0 && node2.underlyingActor.data.timeout - timenow < config.getLong(PaxosActor.leaderTimeoutMaxKey))
  }

  def backdownToFollowerAndRequestRetransmissionOnCommitHigherThanLastCommitted(role: PaxosRole)(implicit sender: ActorRef) {
    require(role == Leader || role == Recoverer)
    val stubJournal: Journal = stub[Journal]
    // given a recoverer with no responses
    val identifier = Identifier(1, BallotNumber(lowValue + 1, 0), 1L)
    val greaterThan = Identifier(1, BallotNumber(lowValue + 2, 2), 2L)
    val timenow = 999L
    val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, stubJournal, ArrayBuffer.empty, None){
      override def clock() = timenow
    })
    val epoch = Some(identifier.number)
    fsm.underlyingActor.setAgent(role, initialData.copy(epoch = epoch, clusterSize = 3, progress = initialData.progress.copy(highestCommitted = identifier)))
    // when it gets commit
    val commit = Commit(greaterThan)
    fsm ! commit
    // it sends a retransmission
    expectMsg(100 millisecond, RetransmitRequest(0, 1, 1L))
    // and returns to be follower
    assert(fsm.underlyingActor.role == Follower)
    // and clears data
    assert(fsm.underlyingActor.data.acceptResponses.isEmpty)
    assert(fsm.underlyingActor.data.prepareResponses.isEmpty)
    assert(fsm.underlyingActor.data.epoch == None)
    // and sets a fresh timeout
    assert(fsm.underlyingActor.data.timeout > 0 && fsm.underlyingActor.data.timeout - timenow < config.getLong(PaxosActor.leaderTimeoutMaxKey))
  }

  def backdownToFollowerAndCommitOnCommitHigherThanLastCommitted(role: PaxosRole)(implicit sender: ActorRef) {
    require(role == Leader || role == Recoverer)
    val stubJournal: Journal = stub[Journal]

    val timenow = 999L

    // given an initialized journal
    (stubJournal.load _) when() returns (Journal.minBookwork)

    // given slot 1 has been accepted under the same number as previously committed slot 0 shown in initialData
    val identifier = Identifier(1, BallotNumber(lowValue, lowValue), 1L)
    val accepted = Accept(identifier, ClientRequestCommandValue(0, expectedBytes))
    (stubJournal.accepted _) when (1L) returns Some(accepted)

    // when we have a leader-like which could commit
    val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, stubJournal, ArrayBuffer.empty, None) {
      override def clock() = timenow
    })

    val epoch = Some(fsm.underlyingActor.data.progress.highestPromised)
    fsm.underlyingActor.setAgent(role, fsm.underlyingActor.data.copy(epoch = epoch))

    // when we see a high commit
    fsm ! Commit(identifier)

    // then it sends no messages
    expectNoMsg(25 millisecond)
    // and delivered that value
    assert(fsm.underlyingActor.delivered.head == ClientRequestCommandValue(0, expectedBytes))
    // and journal bookwork
    (stubJournal.save _).verify(fsm.underlyingActor.data.progress)
    // and returns to be follower
    assert(fsm.underlyingActor.role == Follower)
    // and clears data
    assert(fsm.underlyingActor.data.acceptResponses.isEmpty)
    assert(fsm.underlyingActor.data.prepareResponses.isEmpty)
    assert(fsm.underlyingActor.data.epoch == None)

    // and sets a fresh timeout
    assert(fsm.underlyingActor.data.timeout > 0 && fsm.underlyingActor.data.timeout - timenow < config.getLong(PaxosActor.leaderTimeoutMaxKey))
  }

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
    val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, stubJournal, ArrayBuffer.empty, None) {
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
    val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, tempJournal, ArrayBuffer.empty, None) {
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
    val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, stubJournal, ArrayBuffer.empty, None) {
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