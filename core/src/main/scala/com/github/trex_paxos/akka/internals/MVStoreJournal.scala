package com.github.trex_paxos.akka.internals

import com.github.trex_paxos.akka.TrexMembership
import com.github.trex_paxos.library.{Accept, Identifier, Journal, JournalBounds, PaxosAlgorithm, Progress}
import org.h2.mvstore._

import java.io.{Closeable, File}

/**
 * @param journalFile The file to store data within.
 * @param retained The number of committed log entries to retain for retransmission. Unused if set to Int.MAX_VALUE
 * @param retainedBatchSize The size of the batch of entries to delete to keep when trimming the log based on "retained"
 */
class MVStoreJournal(journalFile: File, retained: Int = Int.MaxValue, retainedBatchSize: Int = 0) extends Journal
    with TrexMembership with Closeable {
  import com.github.trex_paxos.util.{Pickle,ByteChain}

  // whether store needs initializing
  val emptyStoreFile = journalFile.length == 0

  // typically we would not compress as we would expect large payloads to be compressed by the client else written to something like a cloud bucket so the payload only needs to be the url
  val store = new MVStore.Builder().fileName(journalFile.getCanonicalPath()).open();

  // we need only one entry in PAXOS_STATE which is the current node packs Progress
  val stateMap: MVMap[String, Array[Byte]] = store.openMap("PAXOS_STATE")

  // the log of possibly unfixed values at any given node is in the is mutable b-tree map
  val logMap: MVMap[Long, Array[Byte]] = store.openMap("VALUES_LOG")

  // stores the current cluster membership at a given slot
  val memberMap: MVMap[Long, Array[Byte]] = store.openMap("MEMBERS")

  protected def init(): Unit = {
    saveProgress(Journal.minBookwork)
  }

  if (emptyStoreFile) init()

  /**
    * Synchronously journal progress bookwork.
    *
    * @param progress The highest committed and highest promised of this cluster node.
    */
  override def saveProgress(progress: Progress): Unit = {
    // save the progress
    val bytes = Pickle.pickle(progress).prependCrcData()
    stateMap.put("Progress", bytes.toArray)
    store.commit() // eager commit
    // only if we want to delete old values
    if( retained < Int.MaxValue ) {
      // and we have at least retainedBatchSize more than what we want to retain
      if( logMap.size() > retained + retainedBatchSize ) {
        val oldMin = logMap.firstKey()
        val earliestRetained = progress.highestCommitted.logIndex - retained - 1
        val newMin = logMap.floorKey(earliestRetained)
        if( newMin - oldMin > retainedBatchSize ) {
          import scala.jdk.CollectionConverters._
          logMap.cursor(oldMin, newMin, false).asScala foreach {
            key => {
              logMap.remove(key)
            }
          }
        }
        // be lazy so no commit
      }
    }
  }

  /**
    * Load progress bookwork.
    *
    * @return The maximum promise and the highest commit identifier.
    */
  override def loadProgress(): Progress = {
    val bytes = stateMap.get("Progress")
    val checked = ByteChain(bytes).checkCrcData()
    Pickle.unpickleProgress(checked.iterator)
  }

  /**
    * Synchronously save an accept message in its slot specified by the logIndex in the Identifier of the message.
    * Callers *must* ensure that values with a low BallotNumber do not overwrite values with a higher number using
    * the Paxos Algorithm.
    */
  override def accept(a:  Accept*): Unit = {
    if (a.nonEmpty) {
      a foreach {
        case a@Accept(Identifier(_, _, logIndex), _) =>
          val bytes = Pickle.pickle(a).prependCrcData()
          logMap.put(logIndex, bytes.toArray)
      }
      store.commit()
    }
  }

  /**
    * Load the accept message from the specified slot.
    *
    * @return The accept message at the requested slot if any.
    */
  override def accepted(logIndex: Long): Option[Accept] = Option(logMap.get(logIndex)) match {
    case None =>
      None
    case Some(bytes) =>
      val checked = ByteChain(bytes).checkCrcData()
      Some(Pickle.unpickleAccept(checked.iterator))
  }

  /**
    * The lowest and highest index currently available.
    *
    * @return The min and max index retrained within this journal which is subject to the retention policy of the implementation.
    */
  override def bounds(): JournalBounds = {
    if (logMap.isEmpty())
      PaxosAlgorithm.minJournalBounds
    else {
      JournalBounds(logMap.firstKey(), logMap.lastKey())
    }
  }

  val UTF8 = "UTF8"

  override def saveMembership(cm: CommittedMembership): Unit = {
    val lastSlotOption = Option(memberMap.lastKey())
    lastSlotOption foreach {
      case last if last < cm.slot => // good
      case last => throw new IllegalArgumentException(s"slot ${cm.slot} is not higher than last saved: ${last}")
    }
    val json = MemberPickle.toJson(cm)
    memberMap.put(cm.slot, json.getBytes(UTF8))
    store.commit()
    // TODO consider gc of old values
  }

  override def loadMembership(): Option[CommittedMembership] = {
    val lastSlotOption = Option(memberMap.lastKey())
    lastSlotOption map { (s: Long) =>
      val jsonBytesUtf8 = memberMap.get(s)
      MemberPickle.fromJson(new String(jsonBytesUtf8, UTF8)).get
    }
  }

  override def close(): Unit = store.close()
}
