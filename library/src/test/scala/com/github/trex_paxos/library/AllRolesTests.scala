package com.github.trex_paxos.library

import java.util.concurrent.atomic.{AtomicLong, AtomicReference, AtomicBoolean}

import org.scalamock.scalatest.MockFactory
import org.scalatest.{OptionValues, Matchers, Spec}

import scala.collection.mutable.ArrayBuffer

class InMemoryJournal extends Journal {
  val lastSaveTime = new AtomicLong()
  val p = new AtomicReference[(Long, Progress)]()

  override def save(progress: Progress): Unit = {
    val n = System.nanoTime()
    lastSaveTime.set(n)
    p.set((n, progress))
  }

  override def bounds: JournalBounds = JournalBounds(0, 0)

  override def load(): Progress = p.get()._2

  // Map[logIndex,(nanoTs,accept)]
  val a = collection.mutable.Map.empty[Long, (Long, Accept)]

  override def accept(as: Accept*): Unit = as foreach { i =>
    val n = System.nanoTime()
    lastSaveTime.set(n)
    a.put(i.id.logIndex, (n, i))
  }

  override def accepted(logIndex: Long): Option[Accept] = a.get(logIndex).map(_._2)

}

class TestAcceptMapJournal extends Journal {
  var accept: Map[Long, Accept] = Map.empty

  def accept(accepted: Accept*): Unit = accepted foreach { a =>
    accept = accept + (a.id.logIndex -> a)
  }

  def accepted(logIndex: Long): Option[Accept] = {
    accept.get(logIndex)
  }

  def bounds: JournalBounds = {
    val keys = accept.keys
    if (keys.isEmpty) JournalBounds(0L, 0L) else JournalBounds(keys.head, keys.last)
  }

  var progress: Progress = null

  def load(): Progress = progress

  def save(p: Progress): Unit = progress = p
}

class AllRolesTests extends Spec with PaxosLenses with Matchers with OptionValues with MockFactory {

  import TestHelpers._

  def respondsIsNotLeader(role: PaxosRole) {
    require(role != Leader)
    val agent = PaxosAgent(0, role, initialData)
    val sent = ArrayBuffer[PaxosMessage]()
    val io = new UndefinedIO {
      override def send(msg: PaxosMessage): Unit = sent += msg
    }
    val event = new PaxosEvent(io, agent, ClientRequestCommandValue(0L, expectedBytes))
    val paxosAlgorithm = new PaxosAlgorithm
    // when
    val PaxosAgent(_,newRole, newData) = paxosAlgorithm(event)
    // then
    assert(newData == initialData)
    assert(newRole == role)
    sent.headOption.value match {
      case nl: NotLeader => // good
      case f => fail(f.toString)
    }
  }

  def shouldIngoreLatePrepareResponse(role: PaxosRole) {
    val agent1 = PaxosAgent(0, role, initialDataCommittedSlotOne)
    val event1 = PaxosEvent(undefinedIO, agent1, PrepareNack(minPrepare.id, 2, initialData.progress, initialData.progress.highestCommitted.logIndex, Long.MaxValue))
    val agent2@PaxosAgent(_, newRole, data) = paxosAlgorithm(event1)
    newRole shouldBe role
    data shouldBe agent1.data
    val event2 = PaxosEvent(undefinedIO, agent1, PrepareAck(minPrepare.id, 2, initialData.progress, initialData.progress.highestCommitted.logIndex, Long.MaxValue, None))
    val PaxosAgent(_, newRole2, data2) = paxosAlgorithm(event2)
    newRole2 shouldBe role
    data2 shouldBe agent1.data

  }
}
