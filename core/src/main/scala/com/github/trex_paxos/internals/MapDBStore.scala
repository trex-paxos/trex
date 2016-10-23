
package com.github.trex_paxos.internals

import java.io.{Closeable, File}

import org.mapdb.{DB, DBMaker}

import scala.collection.JavaConversions
import com.github.trex_paxos.library._

/**
  * Cluster membership durable store.
  */
trait TrexMembership {
  def saveMembership(cm: CommittedMembership): Unit

  def loadMembership(): Option[CommittedMembership]
}


/**
  * A MapDB storage engine. Note that you must call close on the file for a clean shutdown.
  *
  * @param journalFile File to journal into.
  * @param retained    Minimum number of committed slots to retain for retransmission
  */
class MapDBStore(journalFile: File, retained: Int) extends Journal with TrexMembership with Closeable {

  import com.github.trex_paxos.util.{Pickle,ByteChain}

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
    val bytes = Pickle.pickle(progress)
    bookworkMap.put("FileJournal", bytes.toArray)
    db.commit() // eager commit
    // lazy gc of some old values as dont commit until next update
    val Progress(_, Identifier(_, _, logIndex)) = progress
    JavaConversions.asScalaSet(storeMap.navigableKeySet).takeWhile(_ < logIndex - retained).foreach { i =>
      if (storeMap.containsKey(i))
        storeMap.remove(i)
    }
  }

  def loadProgress(): Progress = {
    val bytes = bookworkMap.get("FileJournal")
    Pickle.unpickleProgress(ByteChain(bytes))
  }

  def accept(a: Accept*): Unit = {
    a foreach {
      case a@Accept(Identifier(_, _, logIndex), value) =>
        // store the value in the map
        val bytes = Pickle.pickle(a)
        storeMap.put(logIndex, bytes.toArray)
    }
    if (a.nonEmpty) db.commit()
  }

  def accepted(logIndex: Long): Option[Accept] = {
    Option(storeMap.get(logIndex)) match {
      case None =>
        None
      case Some(bytes) =>
        Some(Pickle.unpickleAccept(ByteChain(bytes)))
    }
  }

  def close(): Unit = {
    db.close
  }

  def bounds: JournalBounds = {
    // TODO this needs tests
    val keysAscending = storeMap.navigableKeySet()
    if (storeMap.isEmpty())
      Journal.minJournalBounds
    else {
      JournalBounds(keysAscending.iterator().next(), keysAscending.descendingSet().iterator().next())
    }
  }

  // stores the current cluster membership at a given slot
  val memberMap: java.util.concurrent.ConcurrentNavigableMap[Long, Array[Byte]] =
    db.getTreeMap("MEMBERS")

  val UTF8 = "UTF8"

  override def loadMembership(): Option[CommittedMembership] =
  {
    import scala.collection.JavaConverters._
    val lastSlotOption =  memberMap.descendingKeySet().iterator().asScala.toStream.headOption
    lastSlotOption map { (s: Long) =>
      val jsonBytesUtf8 = memberMap.get(s)
      MemberPickle.fromJson(new String(jsonBytesUtf8, UTF8))
    }
  }

  override def saveMembership(cm: CommittedMembership): Unit =
  {
    import scala.collection.JavaConverters._
    val lastSlotOption =  memberMap.descendingKeySet().iterator().asScala.toStream.headOption
    lastSlotOption foreach {
      case last if last < cm.slot => // good
      case last => throw new IllegalArgumentException(s"slot ${cm.slot} is not higher than last ${last}")
    }
    val json = MemberPickle.toJson(cm)
    memberMap.put(cm.slot, json.getBytes(UTF8))
    db.commit()
    // TODO consider gc of old values
  }
}