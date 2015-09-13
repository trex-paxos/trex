package com.github.simbo1905.trex.library

trait CommitHandler[ClientRef] {

  import CommitHandler._

  def journal: Journal

  def nodeUniqueId: Int

  def deliver(value: CommandValue): Any

  def plog: PaxosLogging

  def trace(state: PaxosRole, data: PaxosData[ClientRef], payload: CommandValue): Unit

  /**
   * Attempts to commit up to the log index specified by the slot specified. A committed value is delivered to the application using the deliver callback. Journals and returns a new progress if it is able to commit data previously accepted else returns the unchanged progress. As a leader must change its proposal number if it looses its leadership and must commit in order. This enables the method to cursor through uncommitted slots and commit them in order if they share the same proposal number as the slot being committed.
   * @param state Current node state
   * @param data Current node data
   * @param identifier The value to commit specified by its number and slot position.
   * @param progress The current high watermarks of this node.
   * @return A tuple of the new progress and a seq of the identifiers and the response to the deliver operation.
   */
  def commit(state: PaxosRole, data: PaxosData[ClientRef], identifier: Identifier, progress: Progress): (Progress, Seq[(Identifier, Any)]) = {
    val Identifier(_, number, commitIndex) = identifier
    val Progress(_, highestCommitted) = progress

    val committable: Seq[Accept] = committableValues(number, highestCommitted, commitIndex, (journal.accepted _))

    if (committable.isEmpty) {
      (progress, Seq.empty[(Identifier, Any)])
    } else {
      val results = committable map { a =>
        val bytes = a.value match {
          case noop@NoOperationCommandValue => noop.bytes
          case _ => deliver(a.value)
        }
        (a.id, bytes)
      }

      committable.lastOption match {
        case Some(newHighestCommitted) =>
          val newProgress = Progress.highestCommittedLens.set(progress, newHighestCommitted.id)
          journal.save(newProgress)
          (newProgress, results)
        case x =>
          plog.error(s"this code should be unreachable but found $x")
          (progress, Seq.empty[(Identifier, Any)])
      }
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