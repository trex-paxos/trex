
package com.github.trex_paxos.core

import java.io.{Closeable, File}

import com.github.trex_paxos.{Era, Membership}
import org.mapdb.{DB, DBMaker}

import scala.collection.JavaConversions
import com.github.trex_paxos.library._

/**
  * A MapDB storage engine. Note that you must call close on the file for a clean shutdown.
  *
  * @param journalFile File to journal into.
  * @param retained    Minimum number of committed slots to retain for retransmission
  */
class MapDBStore(journalFile: File, retained: Int) extends Journal with MemberStore with Closeable {

  import com.github.trex_paxos.util.{Pickle, ByteChain}

  // whether store needs initializing
  val emptyStoreFile = journalFile.length == 0

  // MapDB store
  val db: DB = DBMaker.newFileDB(journalFile).make()

  // holds the current bookwork such as highest committed index and other progress
  val bookworkMap: java.util.concurrent.ConcurrentNavigableMap[String, Array[Byte]] =
    db.getTreeMap("BOOKWORK")

  // stores accept messages at a given slot index
  val storeMap: java.util.concurrent.ConcurrentNavigableMap[Long, Array[Byte]] =
    db.getTreeMap("VALUES")

  protected def init(): Unit = {
    saveProgress(Journal.minBookwork)
  }

  if (emptyStoreFile) init()

  def saveProgress(progress: Progress): Unit = {
    // save the bookwork
    val bytes = Pickle.pickle(progress).prependCrcData()
    bookworkMap.put("FileJournal", bytes.toArray)
    db.commit() // eager commit
    // lazy gc of some old values as dont commit until next update
    // TODO remove this logic by simply modulo the slot by the retained so that we over write a fixed number of keys
    val Progress(_, Identifier(_, _, logIndex)) = progress
    JavaConversions.asScalaSet(storeMap.navigableKeySet).takeWhile(_ < logIndex - retained).foreach { i =>
      if (storeMap.containsKey(i))
        storeMap.remove(i)
    }
  }

  def loadProgress(): Progress = {
    // init() method means that this should never return null
    val bytes = bookworkMap.get("FileJournal")
    val checked = ByteChain(bytes).checkCrcData()
    Pickle.unpickleProgress(checked.iterator)
  }

  def accept(a: Accept*): Unit = {
    a foreach {
      case a@Accept(Identifier(_, _, logIndex), value) =>
        // store the value in the map
        val bytes = Pickle.pickle(a).prependCrcData()
        storeMap.put(logIndex, bytes.toArray)
    }
    if (a.nonEmpty) db.commit()
  }

  def accepted(logIndex: Long): Option[Accept] = {
    Option(storeMap.get(logIndex)) match {
      case None =>
        None
      case Some(bytes) =>
        val checked = ByteChain(bytes).checkCrcData()
        Some(Pickle.unpickleAccept(checked.iterator))
    }
  }

  def close(): Unit = {
    db.close
  }

  def bounds: JournalBounds = {
    val keysAscending = storeMap.navigableKeySet()
    if (storeMap.isEmpty())
      Journal.minJournalBounds
    else {
      JournalBounds(keysAscending.iterator().next(), keysAscending.descendingSet().iterator().next())
    }
  }

  // Stores the sequential cluster memberships keyed by the era number.
  val eraMap: java.util.concurrent.ConcurrentNavigableMap[Int, Array[Byte]] =
    db.getTreeMap("MEMBERS")

  val UTF8 = "UTF8"

  override def saveMembership(era: Era): Unit = {
    import scala.collection.JavaConverters._

    era.membership.effectiveSlot match {
      case None => throw new IllegalArgumentException(s"effective slot of membership must not be None as membership must have been committed at a particular slot ${era}")
      case _ => // okay
    }

    val lastSlotOption: Option[Int] = eraMap.descendingKeySet().iterator().asScala.toStream.headOption
    lastSlotOption foreach {
      case last if last == era.era + 1 => // good
      case last => throw new IllegalArgumentException(s"new era number ${era} is not +1 the last era number: ${last}")
    }

    if (lastSlotOption.isEmpty) require(era.era == 0, s"if the store is empty must persist era number 0: ${era}")
    val json = MemberPickle.toJson(era)
    eraMap.put(era.era, json.getBytes(UTF8))
    db.commit()
  }

  override def loadMembership(): Option[Era] = {
    import scala.collection.JavaConverters._
    val lastSlotOption = eraMap.descendingKeySet().iterator().asScala.toStream.headOption
    lastSlotOption map { (e: Int) =>
      val jsonBytesUtf8 = eraMap.get(e)
      val js = new String(jsonBytesUtf8, UTF8)
      MemberPickle.fromJson(js).getOrElse(throw new IllegalArgumentException(js))
    }
  }
}