package com.github.trex_paxos

import com.github.trex_paxos.library._

import scala.collection.immutable.SortedMap
import scala.language.postfixOps

class TestJournal extends Journal {
  private val _progress = Box(Journal.minBookwork.copy())
  private val _map = Box(SortedMap[Long, Accept]())

  def saveProgress(progress: Progress): Unit = _progress(progress)

  def loadProgress(): Progress = _progress()

  def accept(accepted: Accept*): Unit = accepted foreach { a =>
    _map(_map() + (a.id.logIndex -> a))
  }

  def accepted(logIndex: Long): Option[Accept] = _map().get(logIndex)

  def bounds: JournalBounds = {
    val keys = _map().keys
    if (keys.isEmpty) JournalBounds(0L, 0L) else JournalBounds(keys.head, keys.last)
  }
}
