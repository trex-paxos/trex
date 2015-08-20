package com.github.simbo1905.trex.internals

import akka.actor.ActorRef
import akka.testkit.{TestFSMRef, TestKit}
import com.github.simbo1905.trex._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.postfixOps

trait LeaderLikeSpec {
  self: TestKit with MockFactory with AllStateSpec with Matchers =>

  import AllStateSpec._
  import Ordering._
  import PaxosActor.Configuration

  def ignoreCommitMessageLogIndexLessThanLastCommit(state: PaxosRole)(implicit sender: ActorRef) {
    require(state == Leader || state == Recoverer)
    // given a leaderlike with no responses
    val identifier = Identifier(0, BallotNumber(lowValue + 1, 0), 1L)
    val lessThan = Identifier(0, BallotNumber(lowValue, 0), 0L)
    val data = initialData.copy(clusterSize = 3, progress = initialData.progress.copy(highestCommitted = identifier))
    val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, stubJournal, ArrayBuffer.empty, None))
    fsm.setState(state, data)
    // when it gets commit
    val commit = Commit(lessThan)
    fsm ! commit
    // it sends no messages
    expectNoMsg(25 millisecond)
    // and stays a recoverer
    assert(fsm.stateName == state)
  }

  def ignoreCommitMessageSameSlotLowerNodeIdentifier(state: PaxosRole)(implicit sender: ActorRef) {
    // given
    val node2slot1Identifier = Identifier(2, BallotNumber(lowValue + 1, 2), 1L)
    val node2 = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 2, sender, stubJournal, ArrayBuffer.empty, None))
    node2.setState(state, initialData.copy(epoch = Some(node2slot1Identifier.number), clusterSize = 3, progress = initialData.progress.copy(highestCommitted = node2slot1Identifier)))
    // when node2 it gets commit for same slot but lower nodeIdentifier
    val node0slot1Identifier = Identifier(0, BallotNumber(lowValue + 1, 0), 1L)
    val commit = Commit(node0slot1Identifier)
    node2 ! commit
    // it sends no messages
    expectNoMsg(25 millisecond)
    // and stays a recoverer
    assert(node2.stateName == state)
  }

  def backdownToFollowerOnCommitSameSlotHigherNodeIdentifier(state: PaxosRole)(implicit sender: ActorRef) {
    // given
    val node2slot1Identifier = Identifier(2, BallotNumber(lowValue + 1, 2), 1L)
    val timenow = 999L
    val node2 = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 2, sender, stubJournal, ArrayBuffer.empty, None){
      override def clock() = timenow
    })
    node2.setState(state, initialData.copy(epoch = Some(node2slot1Identifier.number), clusterSize = 3, progress = initialData.progress.copy(highestCommitted = node2slot1Identifier)))
    // when node2 it gets commit for same slot but lower nodeIdentifier
    val node3slot1Identifier = Identifier(0, BallotNumber(lowValue + 1, 3), 1L)
    val commit = Commit(node3slot1Identifier)
    node2 ! commit
    // it sends no messages
    expectNoMsg(25 millisecond)
    // and returns to be follower
    assert(node2.stateName == Follower)
    // and clears data
    assert(node2.stateData.acceptResponses.isEmpty)
    assert(node2.stateData.prepareResponses.isEmpty)
    assert(node2.stateData.epoch == None)
    // and sets a fresh timeout
    assert(node2.stateData.timeout > 0 && node2.stateData.timeout - timenow < config.getLong(PaxosActor.leaderTimeoutMaxKey))
  }

  def backdownToFollowerAndRequestRetransmissionOnCommitHigherThanLastCommitted(state: PaxosRole)(implicit sender: ActorRef) {
    // given a recoverer with no responses
    val identifier = Identifier(1, BallotNumber(lowValue + 1, 0), 1L)
    val greaterThan = Identifier(1, BallotNumber(lowValue + 2, 2), 2L)
    val timenow = 999L
    val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, stubJournal, ArrayBuffer.empty, None){
      override def clock() = timenow
    })
    fsm.setState(state, initialData.copy(clusterSize = 3, progress = initialData.progress.copy(highestCommitted = identifier)))
    // when it gets commit
    val commit = Commit(greaterThan)
    fsm ! commit
    // it sends a retransmission
    expectMsg(100 millisecond, RetransmitRequest(0, 1, 1L))
    // and returns to be follower
    assert(fsm.stateName == Follower)
    // and clears data
    assert(fsm.stateData.acceptResponses.isEmpty)
    assert(fsm.stateData.prepareResponses.isEmpty)
    assert(fsm.stateData.epoch == None)
    // and sets a fresh timeout
    assert(fsm.stateData.timeout > 0 && fsm.stateData.timeout - timenow < config.getLong(PaxosActor.leaderTimeoutMaxKey))
  }

  def backdownToFollowerAndCommitOnCommitHigherThanLastCommitted(state: PaxosRole)(implicit sender: ActorRef) {

    val timenow = 999L

    // given an initialized journal
    (stubJournal.load _) when() returns (Journal.minBookwork)

    // given slot 1 has been accepted under the same number as previously committed slot 0 shown in initialData
    val identifier = Identifier(1, BallotNumber(lowValue, lowValue), 1L)
    val accepted = Accept(identifier, ClientRequestCommandValue(0, expectedBytes))
    (stubJournal.accepted _) when (1L) returns Some(accepted)

    // when we have a leader-like which could commit
    val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, stubJournal, ArrayBuffer.empty, None) {
      override def clock() = timenow
    })

    fsm.setState(state)

    // when we see a high commit
    fsm ! Commit(identifier)

    // then it sends no messages
    expectNoMsg(25 millisecond)
    // and delivered that value
    assert(fsm.underlyingActor.delivered.head == ClientRequestCommandValue(0, expectedBytes))
    // and journal bookwork
    (stubJournal.save _).verify(fsm.stateData.progress) // TODO huh?
    // and returns to be follower
    assert(fsm.stateName == Follower)
    // and clears data
    assert(fsm.stateData.acceptResponses.isEmpty)
    assert(fsm.stateData.prepareResponses.isEmpty)
    assert(fsm.stateData.epoch == None)

    // and sets a fresh timeout
    assert(fsm.stateData.timeout > 0 && fsm.stateData.timeout - timenow < config.getLong(PaxosActor.leaderTimeoutMaxKey))
  }

  def resendsNoLeaderChallenges(state: PaxosRole)(implicit sender: ActorRef): Unit = {
    val lastCommitted = Identifier(0, BallotNumber(1, 0), 98L)
    // self voted on accept id99
    val id99 = Identifier(0, BallotNumber(1, 0), 99L)
    val a99 = Accept(id99, ClientRequestCommandValue(0, Array[Byte](1, 1)))
    val votes = TreeMap(id99 -> AcceptResponsesAndTimeout(0L, a99, Map(0 -> AcceptAck(id99, 0, initialData.progress))))
    val responses: PaxosData = PaxosData.acceptResponsesLens.set(initialData, votes)
    val oldProgress = Progress.highestPromisedHighestCommitted.set(responses.progress, (lastCommitted.number, lastCommitted))
    val timenow = 999L
    val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, stubJournal, ArrayBuffer.empty, None) {
      override def clock() = timenow
    })
    fsm.setState(state, responses.copy(epoch = Some(BallotNumber(1, 0)), progress = oldProgress))

    // when it gets a timeout
    fsm ! PaxosActor.CheckTimeout

    // it reboardcasts the accept
    expectMsg(100 millisecond, a99)

    // and sets a fresh timeout
    assert(fsm.stateData.timeout > 0 && fsm.stateData.timeout - timenow < config.getLong(PaxosActor.leaderTimeoutMaxKey))
  }

  // TODO these next few tests need to be DRYed
  def resendsHigherAcceptOnLearningOtherNodeHigherPromise(state: PaxosRole)(implicit sender: ActorRef): Unit = {
    val lastCommitted = Identifier(0, BallotNumber(1, 0), 98L)
    // given a leader who has boardcast slot 99 and seen a nack
    val id99 = Identifier(0, BallotNumber(1, 0), 99L)
    val a99 = Accept(id99, ClientRequestCommandValue(0, Array[Byte](1, 1)))
    val votes = TreeMap(id99 -> AcceptResponsesAndTimeout(0L, a99, Map(
      0 -> AcceptAck(id99, 0, initialData.progress),
      1 -> AcceptNack(id99, 1, initialData.progress.copy(highestPromised = BallotNumber(22, 2)))
    )))
    val responses = PaxosData.acceptResponsesLens.set(initialData, votes)
    val committed = Progress.highestPromisedHighestCommitted.set(responses.progress, (lastCommitted.number, lastCommitted))
    val timenow = 999L
    var sendTime = 0L
    val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, stubJournal, ArrayBuffer.empty, None) {
      override def clock() = timenow
      override def send(actor: ActorRef, msg: Any): Unit = {
        sendTime = System.nanoTime()
        actor ! msg
      }
    })
    fsm.setState(state, responses.copy(progress = committed, epoch = Some(BallotNumber(1, 0))))
    // and a journal which records the save time
    var saveTime = 0L
    (stubJournal.save _) when(*) returns {
      saveTime = System.nanoTime()
      Unit
    }

    // when it gets a timeout
    fsm ! PaxosActor.CheckTimeout

    // it boardcasts a higher accept
    val newEpoch = BallotNumber(23, 0)
    val newIdentifier = Identifier(0, newEpoch, 99L)
    expectMsgPF(100 millisecond) {
      case a: Accept if a.id == newIdentifier && a.value == a99.value =>
      case a: Accept =>

        fail(s"${a.id}, ${newIdentifier} => ${a.id == newIdentifier} || ${a.value}, ${a99.value} => ${a.value == a99.value}")
    }

    // and sets its
    assert(fsm.stateData.epoch == Some(newEpoch))
    // and sets a fresh timeout
    assert(fsm.stateData.timeout > 0 && fsm.stateData.timeout - timenow < config.getLong(PaxosActor.leaderTimeoutMaxKey))

    // and it sent out the messages only after having journalled its own promise
    assert(saveTime != 0 && sendTime != 0 && saveTime < sendTime)
  }

  def resendsHigherAcceptOnHavingMadeAHigherPromiseAtTimeout(state: PaxosRole)(implicit sender: ActorRef): Unit = {
    val lastCommitted = Identifier(0, BallotNumber(1, 0), 98L)
    // given a leader who has boardcast slot 99 and seen a nack
    val id99 = Identifier(0, BallotNumber(1, 0), 99L)
    val a99 = Accept(id99, ClientRequestCommandValue(0, Array[Byte](1, 1)))
    val votes = TreeMap(id99 -> AcceptResponsesAndTimeout(0L, a99, Map(
      0 -> AcceptAck(id99, 0, initialData.progress)
    )))
    val responses = PaxosData.acceptResponsesLens.set(initialData, votes)
    val committed = Progress.highestPromisedHighestCommitted.set(responses.progress, (BallotNumber(22, 2), lastCommitted))
    val timenow = 999L
    val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, stubJournal, ArrayBuffer.empty, None) {
      override def clock() = timenow
      override def freshTimeout(interval: Long): Long = 1234L
    })
    fsm.setState(state, responses.copy(progress = committed, epoch = Some(BallotNumber(1, 0))))

    // when it gets a timeout
    fsm ! PaxosActor.CheckTimeout

    // it boardcasts a higher accept
    val newEpoch = BallotNumber(23, 0)
    val newIdentifier = Identifier(0, newEpoch, 99L)
    expectMsgPF(100 millisecond) {
      case a: Accept if a.id == newIdentifier && a.value == a99.value =>
    }

    // and sets its
    assert(fsm.stateData.epoch == Some(newEpoch))
    // and sets a fresh timeout
    fsm.stateData.timeout shouldBe 1234L
  }

}