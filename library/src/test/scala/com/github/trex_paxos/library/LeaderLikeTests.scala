package com.github.trex_paxos.library

import com.github.trex_paxos.library.TestHelpers._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{OptionValues, Matchers}

import scala.collection.mutable.ArrayBuffer


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
}
