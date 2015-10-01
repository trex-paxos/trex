package com.github.simbo1905.trex.library

import scala.collection.immutable.SortedMap

import Ordering._

trait CommitHandler[RemoteRef] extends PaxosLenses[RemoteRef] {

  import CommitHandler._

  /**
   * Attempts to commit up to the log index specified by the slot specified. A committed value is delivered to the application using the deliver callback. Journals and returns a new progress if it is able to commit data previously accepted else returns the unchanged progress. As a leader must change its proposal number if it looses its leadership and must commit in order. This enables the method to cursor through uncommitted slots and commit them in order if they share the same proposal number as the slot being committed.
   * @param state Current node state
   * @param data Current node data
   * @param identifier The value to commit specified by its number and slot position.
   * @param progress The current high watermarks of this node. // FIXME this is duplicated in data?
   * @return A tuple of the new progress and a seq of the identifiers and the response to the deliver operation.
   */
  def commit(io: PaxosIO[RemoteRef], state: PaxosRole, data: PaxosData[RemoteRef], identifier: Identifier, progress: Progress): (Progress, Seq[(Identifier, Any)]) = {
    val Identifier(_, number, commitIndex) = identifier
    val Progress(_, highestCommitted) = progress

    val committable: Seq[Accept] = committableValues(number, highestCommitted, commitIndex, (io.journal.accepted _))

    if (committable.isEmpty) {
      (progress, Seq.empty[(Identifier, Any)])
    } else {
      val results = committable map { a =>
        val bytes = a.value match {
          case noop@NoOperationCommandValue => noop.bytes
          case _ => io.deliver(a.value)
        }
        (a.id, bytes)
      }

      committable.lastOption match {
        case Some(newHighestCommitted) =>
          val newProgress = Progress.highestCommittedLens.set(progress, newHighestCommitted.id)
          io.journal.save(newProgress)
          (newProgress, results)
        case x =>
          io.plog.error(s"this code should be unreachable but found $x")
          (progress, Seq.empty[(Identifier, Any)])
      }
    }
  }

  def handleCommit(io: PaxosIO[RemoteRef], agent: PaxosAgent[RemoteRef], c: Commit): PaxosAgent[RemoteRef] = {
    val heartbeat = c.heartbeat
    val oldData = agent.data
    val i = c.identifier
    // if the leadership has changed or we see a new heartbeat from the same leader cancel any timeout work
    val newData = heartbeat match {
      case heartbeat if heartbeat > oldData.leaderHeartbeat || i.number > oldData.progress.highestPromised =>
        oldData.copy(leaderHeartbeat = heartbeat, prepareResponses = SortedMap.empty[Identifier, Map[Int, PrepareResponse]], timeout = io.randomTimeout) // TODO lens
      case _ =>
       oldData
    }
    if (i.logIndex <= oldData.progress.highestCommitted.logIndex) {
      // no new commit information in this message
      agent.copy(data = newData)
    } else {
      // attempt a fast-forward commit up to the named slot
      val (newProgress, _) = commit(io, agent.role, oldData, i, newData.progress)
      val newHighestCommitted = newProgress.highestCommitted.logIndex
      // if we did not commit up to the value in the commit message request retransmission of missing values
      if (newHighestCommitted < i.logIndex) {
        io.plog.info("Node {} attempted commit of {} for log index {} found missing accept messages so have only committed up to {} and am requesting retransmission", agent.nodeUniqueId, i, i.logIndex, newHighestCommitted)
        io.send(RetransmitRequest(agent.nodeUniqueId, i.from, newHighestCommitted))
      }
      agent.copy(data = progressLens.set(newData, newProgress))
    }
  }
}

object CommitHandler {
  def committableValues(promise: BallotNumber, highestCommitted: Identifier, commitIndex: Long,
                        journal: (Long) => Option[Accept]): Seq[Accept] = {
    ((highestCommitted.logIndex + 1L) to commitIndex).foldLeft(Seq.empty[Accept]) {
      case (s, i) =>
        journal(i) match {
          case Some(a) if a.id.number == promise => s :+ a
          case _ => return s //use 'return' to bail out of foldLeft early
        }
    }
  }
}