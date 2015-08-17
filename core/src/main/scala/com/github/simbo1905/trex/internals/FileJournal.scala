
package com.github.simbo1905.trex.internals

import java.io.{Closeable, File}

import akka.util.ByteString
import com.github.simbo1905.trex._
import org.mapdb.{DB, DBMaker}

import scala.collection.JavaConversions

/**
 * A file based journal which retains a minimum committed history of values for retransmission. The underlying implementation is a MapDB BTree. 
 *
 * @param storeFile File to journal into. 
 * @param retained Minimum number of committed slots to retain for retransmission
 */
class FileJournal(storeFile: File, retained: Int) extends Journal with Closeable {

  // whether store needs initializing
  val emptyStoreFile = storeFile.length == 0

  // MapDB store
  val db: DB = DBMaker.newFileDB(storeFile).make();

  // holds the current bookwork such as highest committed index and other progress
  val bookworkMap: java.util.concurrent.ConcurrentNavigableMap[String, Array[Byte]] =
    db.getTreeMap("BOOKWORK")

  // stores accept messages at a given slot index
  val storeMap: java.util.concurrent.ConcurrentNavigableMap[Long, Array[Byte]] =
    db.getTreeMap("VALUES")

  // stores the cluster membership at a given slot position
  val clusterMap: java.util.concurrent.ConcurrentNavigableMap[Long, String] =
    db.getTreeMap("CLUSTER")

  // stores accept messages at a given slot index
  val memberMap: java.util.concurrent.ConcurrentNavigableMap[Long, Array[Byte]] =
    db.getTreeMap("MEMBERS")

  protected def init(): Unit = {
    save(Journal.minBookwork)
  }

  if (emptyStoreFile) init()

  def save(progress: Progress): Unit = {
    // save the bookwork
    val bytes = Pickle.pickle(progress)
    bookworkMap.put(this.getClass.getCanonicalName, bytes.toArray)
    db.commit() // eager commit
    // lazy gc of some old values as dont commit until next update
    val Progress(_, Identifier(_, _, logIndex)) = progress
    JavaConversions.asScalaSet(storeMap.navigableKeySet).takeWhile(_ < logIndex - retained).foreach { i =>
      if (storeMap.containsKey(i))
        storeMap.remove(i)
    }
    JavaConversions.asScalaSet(clusterMap.navigableKeySet).takeWhile(_ < logIndex - retained).foreach { i =>
      if (clusterMap.containsKey(i))
        clusterMap.remove(i)
    }
  }

  def load(): Progress = {
    val bytes = bookworkMap.get(this.getClass.getCanonicalName)
    Pickle.unpickleProgress(ByteString(bytes))
  }

  def accept(a: Accept*): Unit = {
    a foreach {
      case a@Accept(Identifier(_, _, logIndex), value) =>
        // store the value in the map
        val bytes = Pickle.pickle(a)
        storeMap.put(logIndex, bytes.toArray)
    }
    if( a.nonEmpty ) db.commit()
  }

  def accepted(logIndex: Long): Option[Accept] = {
    Option(storeMap.get(logIndex)) match {
      case None =>
        None
      case Some(bytes) =>
        Some(Pickle.unpickleAccept(ByteString(bytes)))
    }
  }

  def close(): Unit = {
    db.close
  }

  def bounds: JournalBounds = {
    // TODO this needs tests
    val keysAscending = storeMap.navigableKeySet()
    if (storeMap.isEmpty())
      PaxosActor.minJournalBounds
    else {
      JournalBounds(keysAscending.iterator().next(), keysAscending.descendingSet().iterator().next())
    }
  }
}