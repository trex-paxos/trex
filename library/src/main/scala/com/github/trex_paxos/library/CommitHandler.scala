package com.github.trex_paxos.library

import scala.collection.immutable.SortedMap

import Ordering._

trait CommitHandler extends PaxosLenses {

  import CommitHandler._

  /**
   * Attempts to commit up to the log index specified by the slot specified. A committed value is delivered to the application
   * @param io The IO.
   * @param agent The current agent.
   * @param identifier The value to commit specified by its number and slot position.
   * @return A tuple of the new progress and a seq of the identifiers and the response to the deliver operation.
   */
  def commit(io: PaxosIO, agent: PaxosAgent, identifier: Identifier): (Progress, Seq[(Identifier, Any)]) = {
    val Identifier(_, number, commitIndex) = identifier
    val Progress(_, highestCommitted) = agent.data.progress

    val committable: Seq[Accept] = committableValues(number, highestCommitted, commitIndex, (io.journal.accepted _))

    committable.lastOption match {
      case Some(newHighestCommitted) =>
        val results = committable map { a =>
          val p = Payload(a.id.logIndex, a.value)
          io.logger.debug("Node {} delivering {}", agent.nodeUniqueId, p)
          val bytes = io.deliver(p)
          (a.id, bytes)
        }
        val newProgress = Progress.highestCommittedLens.set(agent.data.progress, newHighestCommitted.id)
        io.journal.save(newProgress)
        (newProgress, results)
      case x =>
        (agent.data.progress, Seq.empty[(Identifier, Any)])
    }

  }

  def handleFollowerCommit(io: PaxosIO, agent: PaxosAgent, c: Commit): PaxosAgent = {
    io.logger.debug("Node {} sees {}", agent.nodeUniqueId, c)
    val heartbeat = c.heartbeat
    val oldData = agent.data
    val i = c.identifier
    // if the leadership has changed or we see a new heartbeat from the same leader cancel any timeout work
    val newData = heartbeat match {
      case heartbeat if heartbeat > oldData.leaderHeartbeat || i.number > oldData.progress.highestCommitted.number =>
        oldData.copy(leaderHeartbeat = heartbeat,
          prepareResponses = SortedMap.empty[Identifier, Map[Int, PrepareResponse]],
          timeout = io.randomTimeout)
      case _ =>
        oldData
    }
    if (i.logIndex <= oldData.progress.highestCommitted.logIndex) {
      // no new commit information in this message
      agent.copy(data = newData)
    } else {
      // attempt a fast-forward commit up to the named slot
      val (newProgress, _) = commit(io, agent, i)
      val newHighestCommitted = newProgress.highestCommitted.logIndex
      // if we did not commit up to the value in the commit message request retransmission of missing values
      if (newHighestCommitted < i.logIndex) {
        io.logger.info("Node {} attempted commit of {} for log index {} found missing accept messages so have only committed up to {} and am requesting retransmission", agent.nodeUniqueId, i, i.logIndex, newHighestCommitted)
        io.send(RetransmitRequest(agent.nodeUniqueId, i.from, newHighestCommitted))
      } else {
        io.logger.debug("Node {} committed up to {}", agent.nodeUniqueId, newHighestCommitted)
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