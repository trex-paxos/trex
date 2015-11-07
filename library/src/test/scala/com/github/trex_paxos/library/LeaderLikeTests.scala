package com.github.trex_paxos.library

import com.github.trex_paxos.library.TestHelpers._
import org.scalatest.Matchers


trait LeaderLikeTests { this: Matchers =>
  def shouldIngoreLowerCommit(role: PaxosRole) {
    val agent = PaxosAgent(0, role, initialDataCommittedSlotOne)
    val event = PaxosEvent(undefinedIO, agent, Commit(Identifier(0, BallotNumber(lowValue, lowValue), 0L), Long.MinValue))
    val PaxosAgent(_, newRole, data) = paxosAlgorithm(event)
    newRole shouldBe role
    data shouldBe agent.data
  }
  def shouldIngoreCommitMessageSameSlotLowerNodeId(role: PaxosRole) {
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
}
