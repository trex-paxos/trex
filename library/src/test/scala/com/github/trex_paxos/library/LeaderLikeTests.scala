package com.github.trex_paxos.library

import java.util.concurrent.atomic.AtomicLong

import com.github.trex_paxos.library.TestHelpers._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{OptionValues, Matchers}

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer
import Ordering._

import scala.compat.Platform

trait LeaderLikeTests { this: Matchers with MockFactory with OptionValues =>
  def shouldIngoreLowerCommit(role: PaxosRole) {
    require(role == Leader || role == Recoverer)
    val agent = PaxosAgent(0, role, initialDataCommittedSlotOne)
    val event = PaxosEvent(undefinedIO, agent, Commit(Identifier(0, BallotNumber(lowValue, lowValue), 0L), Long.MinValue))
    val PaxosAgent(_, newRole, data) = paxosAlgorithm(event)
    newRole shouldBe role
    data shouldBe agent.data
  }
  def shouldIngoreCommitMessageSameSlotLowerNodeId(role: PaxosRole) {
    require(role == Leader || role == Recoverer)
    val node2slot1Identifier = Identifier(2, BallotNumber(lowValue + 1, 2), 1L)
    val agent = PaxosAgent(0, role, initialData.copy(epoch = Some(node2slot1Identifier.number),
      progress = initialData.progress.copy(highestCommitted = node2slot1Identifier)))
    // when node2 it gets commit for same slot but lower nodeIdentifier
    val node0slot1Identifier = Identifier(0, BallotNumber(lowValue + 1, 0), 1L)
    val commit = Commit(node0slot1Identifier)
    val event = PaxosEvent(undefinedIO, agent, commit)
    val PaxosAgent(_, newRole, data) = paxosAlgorithm(event)
    newRole shouldBe role
    data shouldBe agent.data
  }
  def shouldBackdownOnCommitSameSlotHigherNodeId(role: PaxosRole) {
    require(role == Leader || role == Recoverer)
    val node2slot1Identifier = Identifier(2, BallotNumber(lowValue + 1, 2), 1L)
    val node3slot1Identifier = Identifier(0, BallotNumber(lowValue + 1, 3), 1L)
    val agent = PaxosAgent(0, role, initialData.copy(epoch = Some(node2slot1Identifier.number),
      progress = initialData.progress.copy(highestCommitted = node2slot1Identifier)))
    // when node2 it gets commit for same slot but lower nodeIdentifier
    val commit = Commit(node3slot1Identifier)
    val io = new UndefinedIO with SilentLogging {
      override def randomTimeout: Long = 12345L
    }
    val event = PaxosEvent(io, agent, commit)
    val PaxosAgent(_, newRole, data) = paxosAlgorithm(event)
    newRole shouldBe Follower
    data.timeout shouldBe 12345L
    data.leaderHeartbeat should not be(initialData.leaderHeartbeat)
  }
  def shouldBackdownOnHigherSlotCommit(role: PaxosRole) {
    require(role == Leader || role == Recoverer)
    val identifier = Identifier(1, BallotNumber(lowValue + 1, 0), 1L)
    val greaterThan = Identifier(1, BallotNumber(lowValue + 2, 2), 2L)
    val agent = PaxosAgent(0, role, initialData.copy(epoch = Some(identifier.number),
      progress = initialData.progress.copy(highestCommitted = identifier)))
    // empty journal
    val stubJournal: Journal = stub[Journal]
    (stubJournal.accepted _) when(*) returns (None)
    // recording of sends
    val messages: ArrayBuffer[PaxosMessage] = ArrayBuffer()
    val io = new UndefinedIO with SilentLogging {
      override def randomTimeout: Long = 12345L

      override def journal: Journal = stubJournal

      override def send(msg: PaxosMessage): Unit = messages += msg
    }
    // and commit
    val commit = Commit(greaterThan)
    val event = PaxosEvent(io, agent, commit)
    val PaxosAgent(_, newRole, data) = paxosAlgorithm(event)
    newRole shouldBe Follower
    data.timeout shouldBe 12345L
    data.leaderHeartbeat should not be(initialData.leaderHeartbeat)
    messages.headOption.value match {
      case RetransmitRequest(0, 1, 1L) => // good
      case f => fail(f.toString)
    }
  }
  def shouldBackdownAndCommitOnHigherSlotCommit(role: PaxosRole) {
    require(role == Leader || role == Recoverer)
    // given slot 1 has been accepted under the same number as previously committed slot 0 shown in initialData
    val identifier = Identifier(1, BallotNumber(lowValue, lowValue), 1L)
    val value = ClientRequestCommandValue(0, expectedBytes)
    val accepted = Accept(identifier, value)
    val stubJournal: Journal = stub[Journal]
    (stubJournal.accepted _) when(*) returns (Some(accepted))

    val agent = PaxosAgent(0, role, initialData.copy(epoch = Some(initialData.progress.highestPromised)))
    // recording of sends
    val delivered: ArrayBuffer[CommandValue] = ArrayBuffer()
    val io = new UndefinedIO with SilentLogging {
      override def randomTimeout: Long = 12345L

      override def journal: Journal = stubJournal

      override def deliver(value: CommandValue): Any = delivered += value
    }
    // and commit
    val commit = Commit(identifier)
    val event = PaxosEvent(io, agent, commit)
    val PaxosAgent(_, newRole, data) = paxosAlgorithm(event)
    newRole shouldBe Follower
    data.timeout shouldBe 12345L
    data.leaderHeartbeat should not be(initialData.leaderHeartbeat)
    delivered.headOption.value match {
      case `value` => // good
      case f => fail(f.toString)
    }
  }
  def shouldReissueSameAcceptMessageIfTimeoutNoChallenge(role: PaxosRole) {
    require(role == Leader || role == Recoverer)
    // self voted on accept id99
    val lastCommitted = Identifier(0, BallotNumber(1, 0), 98L)
    val id99 = Identifier(0, BallotNumber(1, 0), 99L)
    val a99 = Accept(id99, ClientRequestCommandValue(0, Array[Byte](1, 1)))
    val votes = TreeMap(id99 -> AcceptResponsesAndTimeout(0L, a99, Map(0 -> AcceptAck(id99, 0, initialData.progress))))
    val responses = acceptResponsesLens.set(initialData, votes)
    val oldProgress = Progress.highestPromisedHighestCommitted.set(responses.progress, (lastCommitted.number, lastCommitted))
    val timeNow = 999L
    val agent = PaxosAgent(0, role, responses.copy(epoch = Some(BallotNumber(1, 0)), progress = oldProgress))
    // recording of sends
    val sent: ArrayBuffer[PaxosMessage] = ArrayBuffer()
    val io = new UndefinedIO with SilentLogging {

      override def clock: Long = timeNow

      override def randomTimeout: Long = 12345L

      override def send(msg: PaxosMessage): Unit = sent += msg
    }
    // when
    val event = PaxosEvent(io, agent, CheckTimeout)
    val PaxosAgent(_, newRole, data) = paxosAlgorithm(event)
    // then
    newRole shouldBe role
    sent.headOption.value shouldBe a99
    data.timeout shouldBe 12345L
  }
  def sendHigherAcceptOnLearningOtherNodeHigherPromise(role: PaxosRole){
    require(role == Leader || role == Recoverer)
    // given a leader who has boardcast slot 99 and seen a nack with higher promise
    val tempJournal = new InMemoryJournal()
    val lastCommitted = Identifier(0, BallotNumber(1, 0), 98L)
    val id99 = Identifier(0, BallotNumber(1, 0), 99L)
    val a99 = Accept(id99, ClientRequestCommandValue(0, Array[Byte](1, 1)))
    val votes = TreeMap(id99 -> AcceptResponsesAndTimeout(0L, a99, Map(
      0 -> AcceptAck(id99, 0, initialData.progress),
      1 -> AcceptNack(id99, 1, initialData.progress.copy(highestPromised = BallotNumber(22, 2)))
    )))
    val responses = acceptResponsesLens.set(initialData, votes)
    val committed = Progress.highestPromisedHighestCommitted.set(responses.progress, (lastCommitted.number, lastCommitted))
    val agent = PaxosAgent(0, role, responses.copy(progress = committed, epoch = Some(BallotNumber(1, 0))))
    // recording of sends
    val sent: ArrayBuffer[PaxosMessage] = ArrayBuffer()
    val sentTime = new AtomicLong()
    val io = new UndefinedIO with SilentLogging {
      override def randomTimeout: Long = 12345L

      override def send(msg: PaxosMessage): Unit = {
        sentTime.set(System.nanoTime)
        sent += msg
      }

      override def clock: Long = 0

      override def journal: Journal = tempJournal
    }
    // when
    val event = PaxosEvent(io, agent, CheckTimeout)
    val PaxosAgent(_, newRole, data) = paxosAlgorithm(event)
    // then
    newRole shouldBe role
    // and it should have +1 the high promise it learnt about
    val newEpoch = BallotNumber(23, 0)
    val newIdentifier = Identifier(0, newEpoch, 99L)
    sent.headOption.value match {
      case a: Accept if a.id == newIdentifier && a.value == a99.value =>
      case a: Accept =>
        fail(s"${a.id}, ${newIdentifier} => ${a.id == newIdentifier} || ${a.value}, ${a99.value} => ${a.value == a99.value}")
      case x => fail(x.toString)
    }
    data.epoch shouldBe Some(newEpoch)
    data.timeout shouldBe 12345L
    val saveTime = tempJournal.p.get()._1
    assert(saveTime != 0 && sentTime.get() != 0 && saveTime < sentTime.get())
  }
  def sendsHigherAcceptOnHavingMadeAHigherPromiseAtTimeout(role: PaxosRole) {
    // given a leader who has boardcast slot 99 and seen no other responses
    // but has issued a higher promise to some other node
    val tempJournal = new InMemoryJournal()
    val lastCommitted = Identifier(0, BallotNumber(1, 0), 98L)
    val id99 = Identifier(0, BallotNumber(1, 0), 99L)
    val a99 = Accept(id99, ClientRequestCommandValue(0, Array[Byte](1, 1)))
    val votes = TreeMap(id99 -> AcceptResponsesAndTimeout(0L, a99, Map(
      0 -> AcceptAck(id99, 0, initialData.progress)
    )))
    val responses = acceptResponsesLens.set(initialData, votes)
    val committed = Progress.highestPromisedHighestCommitted.set(responses.progress, (BallotNumber(22, 2), lastCommitted))
    val timeNow = 999L
    val agent = PaxosAgent(0, role, responses.copy(progress = committed, epoch = Some(BallotNumber(1, 0))))
    // recording of sends
    val sent: ArrayBuffer[PaxosMessage] = ArrayBuffer()
    val sentTime = new AtomicLong()
    val io = new UndefinedIO with SilentLogging {

      override def clock: Long = timeNow

      override def randomTimeout: Long = 12345L

      override def send(msg: PaxosMessage): Unit = {
        sentTime.set(System.nanoTime())
        sent += msg
      }

      override def journal: Journal = tempJournal
    }
    // when
    val event = PaxosEvent(io, agent, CheckTimeout)
    val PaxosAgent(_, newRole, data) = paxosAlgorithm(event)
    // then
    newRole shouldBe role
    // it broadcasts higher accept
    val newEpoch = BallotNumber(23, 0)
    val newIdentifier = Identifier(0, newEpoch, 99L)
    sent.headOption.value match {
      case a: Accept if a.id == newIdentifier && a.value == a99.value =>
      case x => fail(x.toString)
    }
    // and increments it epoch
    data.epoch shouldBe Some(newEpoch)
    data.timeout shouldBe 12345L
    val saveTime = tempJournal.lastSaveTime.get()
    assert(saveTime != 0 && sentTime.get() != 0 && saveTime < sentTime.get())
  }

}
