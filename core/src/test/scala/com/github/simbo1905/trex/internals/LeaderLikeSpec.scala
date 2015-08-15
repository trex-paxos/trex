package com.github.simbo1905.trex.internals

import akka.testkit.TestKit
import com.github.simbo1905.trex.internals.PaxosActor._
import org.scalamock.scalatest.MockFactory
import com.github.simbo1905.trex._
import akka.testkit.TestFSMRef

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import akka.actor.ActorRef
import scala.language.postfixOps

trait LeaderLikeSpec {
  self: TestKit with MockFactory with AllStateSpec =>

  import AllStateSpec._
  import PaxosActor.Configuration
  import Ordering._

  def ignoreCommitMessageLessThanLastCommit(state: PaxosRole)(implicit sender: ActorRef) {
    require(state == Leader || state == Recoverer)
    // given a leaderlike with no responses
    val identifier = Identifier(0, BallotNumber(lowValue + 1, 0), 1L)
    val lessThan = Identifier(0, BallotNumber(lowValue, 0), 0L)
    val prepare = Prepare(identifier)
    val initialPrepareResponses = Some((prepare, Map.empty[Int, PrepareResponse]))
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

  def ignoreCommitMessageEqualToLast(state: PaxosRole)(implicit sender: ActorRef) {
    // given a recoverer with no responses
    val identifier = Identifier(0, BallotNumber(lowValue + 1, 0), 1L)
    val equalTo = Identifier(0, BallotNumber(lowValue + 1, 0), 1L)
    val prepare = Prepare(identifier)
    val initialPrepareResponses = Some((prepare, Map.empty[Int, PrepareResponse]))
    val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, stubJournal, ArrayBuffer.empty, None))
    fsm.setState(state, initialData.copy(clusterSize = 3, progress = initialData.progress.copy(highestCommitted = identifier)))
    // when it gets commit
    val commit = Commit(equalTo)
    fsm ! commit
    // it sends no messages
    expectNoMsg(25 millisecond)
    // and stays a recoverer
    assert(fsm.stateName == state)
  }

  def backdownToFollowerAndRequestRetransmissionOnCommitHigherThanLastCommitted(state: PaxosRole)(implicit sender: ActorRef) {
    // given a recoverer with no responses
    val identifier = Identifier(1, BallotNumber(lowValue + 1, 0), 1L)
    val greaterThan = Identifier(1, BallotNumber(lowValue + 2, 2), 2L)
    val prepare = Prepare(identifier)
    val initialPrepareResponses = Some((prepare, Map.empty[Int, PrepareResponse]))
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
    val votes = TreeMap(id99 -> Some(Map(0 -> AcceptAck(id99, 0, initialData.progress))))
    val responses: PaxosData = PaxosData.acceptResponsesLens.set(initialData, votes)
    val oldProgress = Progress.highestPromisedHighestCommitted.set(responses.progress, (lastCommitted.number, lastCommitted))
    val timenow = 999L
    val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, stubJournal, ArrayBuffer.empty, None) {
      override def clock() = timenow
    })
    fsm.setState(state, responses.copy(epoch = Some(BallotNumber(1, 0)), progress = oldProgress))
    // and the accept in the store
    val accept = Accept(id99, ClientRequestCommandValue(0, expectedBytes))
    (stubJournal.accepted _) when (99L) returns Some(accept)

    // when it gets a timeout
    fsm ! PaxosActor.CheckTimeout

    // it reboardcasts the accept
    expectMsg(100 millisecond, accept)

    // FIXME and sets a fresh timeout
  }

  // TODO these next few tests need to be DRYed
  def resendsHigherAcceptOnLearningOtherNodeHigherPromise(state: PaxosRole)(implicit sender: ActorRef): Unit = {
    val lastCommitted = Identifier(0, BallotNumber(1, 0), 98L)
    // given a leader who has boardcast slot 99 and seen a nack
    val id99 = Identifier(0, BallotNumber(1, 0), 99L)
    val votes = TreeMap(id99 -> Some(Map(
      0 -> AcceptAck(id99, 0, initialData.progress),
      1 -> AcceptNack(id99, 1, initialData.progress.copy(highestPromised = BallotNumber(22, 2)))
    )))
    val responses = PaxosData.acceptResponsesLens.set(initialData, votes)
    val committed = Progress.highestPromisedHighestCommitted.set(responses.progress, (lastCommitted.number, lastCommitted))
    val timenow = 999L
    // FIXME use a test file journal and ensure that it calls save in the correct order to sending messages
    val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, stubJournal, ArrayBuffer.empty, None) {
      override def clock() = timenow
    })
    fsm.setState(state, responses.copy(progress = committed, epoch = Some(BallotNumber(1, 0))))
    // and the accept in the store
    val accept = Accept(id99, ClientRequestCommandValue(0, expectedBytes))
    (stubJournal.accepted _) when (99L) returns Some(accept)

    // when it gets a timeout
    fsm ! PaxosActor.CheckTimeout

    // it boardcasts a higher accept
    val newEpoch = BallotNumber(23, 0)
    val newIdentifier = Identifier(0, newEpoch, 99L)
    expectMsgPF(100 millisecond) {
      case a: Accept if a.id == newIdentifier && a.value == accept.value =>
    }

    // and sets its
    assert(fsm.stateData.epoch == Some(newEpoch))
    // FIXME and should increment timeout
  }

  def resendsHigherAcceptOnHavingMadeAHigherPromiseAtTimeout(state: PaxosRole)(implicit sender: ActorRef): Unit = {
    val lastCommitted = Identifier(0, BallotNumber(1, 0), 98L)
    // given a leader who has boardcast slot 99 and seen a nack
    val id99 = Identifier(0, BallotNumber(1, 0), 99L)
    val votes = TreeMap(id99 -> Some(Map(
      0 -> AcceptAck(id99, 0, initialData.progress)
    )))
    val responses = PaxosData.acceptResponsesLens.set(initialData, votes)
    val committed = Progress.highestPromisedHighestCommitted.set(responses.progress, (BallotNumber(22, 2), lastCommitted))
    val timenow = 999L
    val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, stubJournal, ArrayBuffer.empty, None) {
      override def clock() = timenow
    })
    fsm.setState(state, responses.copy(progress = committed, epoch = Some(BallotNumber(1, 0))))
    // and the accept in the store
    val accept = Accept(id99, ClientRequestCommandValue(0, expectedBytes))
    (stubJournal.accepted _) when (99L) returns Some(accept)

    // when it gets a timeout
    fsm ! PaxosActor.CheckTimeout

    // it boardcasts a higher accept
    val newEpoch = BallotNumber(23, 0)
    val newIdentifier = Identifier(0, newEpoch, 99L)
    expectMsgPF(100 millisecond) {
      case a: Accept if a.id == newIdentifier && a.value == accept.value =>
    }

    // and sets its
    assert(fsm.stateData.epoch == Some(newEpoch))
    // FIXME and sets a fresh timeout
  }

}