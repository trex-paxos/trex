
package com.github.trex_paxos.core

import java.io.{Closeable, File}

import com.github.trex_paxos.ClusterConfiguration
import com.github.trex_paxos.library._
import org.mapdb.{DB, DBMaker}

import scala.collection.JavaConversions

/**
  * A MapDB storage engine. Note that you must call close on the file for a clean shutdown.
  *
  * @param journalFile File to journal into.
  * @param retained    Minimum number of committed slots to retain for retransmission
  */
class MapDBStore(journalFile: File, retained: Int) extends Journal with ClusterConfigurationStore with Closeable {

  import com.github.trex_paxos.util.{ByteChain, Pickle}

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
  val clusterConfigByEraMap: java.util.concurrent.ConcurrentNavigableMap[Int, Array[Byte]] =
    db.getTreeMap("CLUSTER_CONFIG")

  val UTF8 = "UTF8"

  override def saveAndAssignEra(clusterConfiguration: ClusterConfiguration): ClusterConfiguration = {
    import scala.collection.JavaConverters._

    // check that the new effectiveSlot is set
    if( clusterConfiguration.effectiveSlot.isEmpty )
      throw new IllegalArgumentException(s"effective slot of membership must not be None as membership must have been committed at a particular slot: ${clusterConfiguration}")

    val lastEraOpt = clusterConfigByEraMap.descendingKeySet().iterator().asScala.toStream.headOption

    // check that initialisation of the store is from effective slot of 0
    lastEraOpt match {
      case None => if( clusterConfiguration.effectiveSlot == Some(0))
        throw new IllegalArgumentException(s"initalisation of memberstore must be from effective slot 0: ${clusterConfiguration}")
      case _ =>
    }

    // check that the new effective slot is not lower than the last known to be committed
    val latestEra: Int = clusterConfigByEraMap.descendingKeySet().iterator().asScala.toStream.headOption match {
      case Some(era) =>
        MemberPickle.fromJson(new String(clusterConfigByEraMap.get(era), "UTF8")) foreach {
          c => if( c.effectiveSlot.getOrElse(-1L) <  clusterConfiguration.effectiveSlot.getOrElse(-1L) ) // note code above forces "not None" so these checks never see -1
            throw new IllegalArgumentException(s"new effective slot must be higher than last. new: ${clusterConfiguration}, old: ${c}")
        }
        era
      case _ => -1
    }

    val clusterConfigurationWithEra = clusterConfiguration.copy(era = Option(latestEra + 1))

    val json = MemberPickle.toJson(clusterConfigurationWithEra)
    clusterConfigByEraMap.put(clusterConfiguration.era.getOrElse(throw new IllegalArgumentException(s"no era ${clusterConfiguration}")), json.getBytes(UTF8))
    db.commit()
    clusterConfigurationWithEra
  }

  override def loadForHighestEra(): Option[ClusterConfiguration] = {
    import scala.collection.JavaConverters._
    val lastSlotOption = clusterConfigByEraMap.descendingKeySet().iterator().asScala.toStream.headOption
    lastSlotOption map { (e: Int) =>
      val jsonBytesUtf8 = clusterConfigByEraMap.get(e)
      val js = new String(jsonBytesUtf8, UTF8)
      MemberPickle.fromJson(js).getOrElse(throw new IllegalArgumentException(js))
    }
  }
}