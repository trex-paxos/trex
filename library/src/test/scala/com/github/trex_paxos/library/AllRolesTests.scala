package com.github.trex_paxos.library

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import org.scalamock.scalatest.MockFactory
import org.scalatest.refspec.RefSpec
import org.scalatest._
import matchers.should._

import scala.collection.mutable.ArrayBuffer

class InMemoryJournal extends Journal {
  val lastSaveTime = new AtomicLong()
  val p: Box[(Long, Progress)] = new Box(None)

  override def saveProgress(progress: Progress): Unit = {
    val n = System.nanoTime()
    lastSaveTime.set(n)
    p((n, progress))
  }

  override def bounds(): JournalBounds = if( a.isEmpty ) {
    JournalBounds(0, 0)
  } else {
    val slots = a.keySet
    JournalBounds(slots.min, slots.max)
  }

  override def loadProgress(): Progress = p() match {
    case (_, p) => p
    case f => throw new IllegalArgumentException(f.toString)
  }

  // Map[logIndex,(nanoTs,accept)]
  val a = collection.mutable.Map.empty[Long, (Long, Accept)]

  override def accept(as: Accept*): Unit = as foreach { i =>
    val n = System.nanoTime()
    lastSaveTime.set(n)
    a.put(i.id.logIndex, (n, i))
  }

  override def accepted(logIndex: Long): Option[Accept] = a.get(logIndex).map {
    case (_, a) => a
    case f => throw new AssertionError(f.toString())
  }

}

class TestAcceptMapJournal extends Journal {
  val accept = Box(Map[Long, Accept]())

  def accept(accepted: Accept*): Unit = accepted foreach { a =>
    accept(accept() + (a.id.logIndex -> a))
  }

  def accepted(logIndex: Long): Option[Accept] = {
    accept().get(logIndex)
  }

  def bounds(): JournalBounds = {
    val keys = accept().keys
    if (keys.isEmpty) JournalBounds(0L, 0L) else JournalBounds(keys.head, keys.last)
  }

  val progress: Box[Progress] = new Box(None)

  def loadProgress(): Progress = progress()

  def saveProgress(p: Progress): Unit = progress(p)
}

class AllRolesTests extends RefSpec with PaxosLenses with Matchers with OptionValues with MockFactory {

  import TestHelpers._

  def respondsIsNotLeader(role: PaxosRole): Unit = {
    require(role != Leader)
    val agent = PaxosAgent(0, role, initialData, initialQuorumStrategy)
    val sent = ArrayBuffer[PaxosMessage]()
    val io = new UndefinedIO {
      override def send(msg: PaxosMessage): Unit = sent += msg

      override def deliver(payload: Payload): Any = {}
    }
    val event = new PaxosEvent(io, agent, DummyCommandValue("1"))
    val paxosAlgorithm = new PaxosAlgorithm
    // when
    val PaxosAgent(_,newRole, newData, _) = paxosAlgorithm(event)
    // then
    assert(newData == initialData)
    assert(newRole == role)
    sent.headOption.value match {
      case nl: NotLeader => // good
      case f => fail(f.toString)
    }
  }

  def shouldIngoreLatePrepareResponse(role: PaxosRole): Unit = {
    val paxosAlgorithm = new PaxosAlgorithm
    val agent1 = PaxosAgent(0, role, initialDataCommittedSlotOne, initialQuorumStrategy)
    val event1 = PaxosEvent(undefinedIO, agent1, PrepareNack(minPrepare.id, 2, initialData.progress, initialData.progress.highestCommitted.logIndex, Long.MaxValue))
    val PaxosAgent(_, newRole, data, _) = paxosAlgorithm(event1)
    newRole shouldBe role
    data shouldBe agent1.data
    val event2 = PaxosEvent(undefinedIO, agent1, PrepareAck(minPrepare.id, 2, initialData.progress, initialData.progress.highestCommitted.logIndex, Long.MaxValue, None))
    val PaxosAgent(_, newRole2, data2, _) = paxosAlgorithm(event2)
    newRole2 shouldBe role
    data2 shouldBe agent1.data

  }
}
