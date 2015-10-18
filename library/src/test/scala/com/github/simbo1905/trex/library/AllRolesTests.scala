package com.github.simbo1905.trex.library

import org.scalatest.{OptionValues, Matchers, Spec}

import scala.collection.mutable.ArrayBuffer

class AllRolesTests extends Spec with PaxosLenses[DummyRemoteRef] with Matchers with OptionValues {
  import TestHelpers._

  def nackLowPrepare(role: PaxosRole) = {
    // given
    val highestAccepted = 909L
    val highPromise = highestPromisedHighestCommittedLens.set(initialData, (BallotNumber(Int.MaxValue, Int.MaxValue), initialData.progress.highestCommitted))
    val agent = PaxosAgent(0, role, highPromise)
    val sent = ArrayBuffer[PaxosMessage]()
    val io = new UndefinedIO {
      override def send(msg: PaxosMessage): Unit = sent += msg

      override def journal: Journal = new UndefinedJournal {
        override def bounds: JournalBounds = JournalBounds(0, highestAccepted)
      }
    }
    val event = new PaxosEvent[DummyRemoteRef](io, agent, prepare)
    val paxosAlgorithm = new PaxosAlgorithm[DummyRemoteRef]
    // when
    val PaxosAgent(_, _, data) = paxosAlgorithm(event)
    // then
    val actualNacks = (sent.collect {
      case p: PrepareNack => Option(p)
      case _ => None
    }).flatten
    actualNacks.size shouldBe 1
    actualNacks.headOption.value shouldBe PrepareNack(prepare.id, 0, agent.data.progress, highestAccepted, agent.data.leaderHeartbeat)
  }
}
