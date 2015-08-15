package com.github.simbo1905.trex.internals

import akka.event.LoggingAdapter
import com.github.simbo1905.trex.Journal
import com.github.simbo1905.trex.JournalBounds
import com.github.simbo1905.trex.internals.PaxosActor.HighestCommittedIndex

case class Retransmission(newProgress: Progress, accepts: Seq[Accept], committed: Seq[CommandValue])

trait RetransmitHandler {

  import RetransmitHandler._

  def journal: Journal

  def nodeUniqueId: Int

  def deliver(value: CommandValue): Any

  def log: LoggingAdapter

  def handleRetransmitResponse(response: RetransmitResponse, nodeData: PaxosData): Progress = {
    // pure functional computation
    val retransmission = processRetransmitResponse(response, nodeData)

    // crash safety relies upon the following side effects happening in the correct order
    retransmission.committed.filter(_ != NoOperationCommandValue).foreach(deliver)

    journal.save(retransmission.newProgress)
    journal.accept(retransmission.accepts: _*)

    retransmission.newProgress
  }

  /**
   * Computes the contiguous committable messages, all messages that may be accepted and the corresponding new progress.
   * If the accept messages are not in log index order they will not be processed which is a bug on send which will
   * halt the progress of the receiving node.  
   * @return
   */
  def processRetransmitResponse(response: RetransmitResponse, nodeData: PaxosData): Retransmission = {
    val highestCommitted = nodeData.progress.highestCommitted
    val highestPromised = nodeData.progress.highestPromised

    // drop all the committed values which are not above our current highest committed log index
    // commit : note sender must ensure sequences are ordered by log index
    val aboveCommitted = response.committed.dropWhile(a => a.id.logIndex <= highestCommitted.logIndex)

    // compute the longest run of values contiguous with our current highest committed
    val commitState = contiguousCommittableCommands(highestCommitted, aboveCommitted)
    val committedCount = commitState.committed.length

    // take the rest of the accept messages we could not commit
    val uncommittable: Seq[Accept] = response.uncommitted ++ aboveCommitted.drop(committedCount)

    // check each ballot number against latest promise and raise promise as necessary
    val acceptState = acceptableAndPromiseNumber(highestPromised, uncommittable)

    // update our progress with new highest commit index and new promise
    val newProgress = Progress.highestPromisedHighestCommitted.set(nodeData.progress, (acceptState.highest, commitState.highestCommitted))

    log.info("Node " + nodeUniqueId + " RetransmitResponse committed {} of {} and accepted {} of {}", committedCount, aboveCommitted.size, acceptState.acceptable.size, response.uncommitted.size)

    // return new progress, the accepts that we should journal so that we may retransmit on request, and the committed values
    Retransmission(newProgress, (aboveCommitted ++ acceptState.acceptable).distinct, commitState.committed.map(_.value))
  }

  def handleRetransmitRequest(request: RetransmitRequest, nodeData: PaxosData): Option[RetransmitResponse] = {
    // extract who to respond to, where they are requesting from and where we are committed upto
    val RetransmitRequest(to, _, requestedLogIndex) = request
    val HighestCommittedIndex(committedLogIndex) = nodeData

    // send the response based on what we have in our journal, access to the journal, and where they have committed
    processRetransmitRequest(journal.bounds, committedLogIndex, journal.accepted _, requestedLogIndex) map {
      case ResponseState(committed, uncommitted) =>
        RetransmitResponse(nodeUniqueId, to, committed, uncommitted)
    }
  }
}

object RetransmitHandler {

  case class CommitState(highestCommitted: Identifier, committed: Seq[Accept] = Seq())

  def contiguousCommittableCommands(highestCommitted: Identifier, accepts: Seq[Accept]): CommitState = {
    val startState = CommitState(highestCommitted, Seq())

    accepts.foldLeft(startState) {
      case (s, a) if a.id.logIndex == s.highestCommitted.logIndex + 1 => CommitState(a.id, s.committed :+ a)
      case (s, a) => return s // use 'return' to bail out of foldLeft early
    }
  }

  case class AcceptState(highest: BallotNumber, acceptable: Seq[Accept] = Seq())

  def acceptableAndPromiseNumber(h: BallotNumber, uncommittable: Seq[Accept]): AcceptState = uncommittable.foldLeft(AcceptState(h)) {
    case (AcceptState(highest, acceptable), a) if a.id.number >= highest => AcceptState(a.id.number, acceptable :+ a)
    case (s, a) => s
  }

  case class ResponseState(committed: Seq[Accept], uncommitted: Seq[Accept])

  def processRetransmitRequest(minMax: JournalBounds, committedLogIndex: Long, journaled: (Long) => Option[Accept], requestedFromLogIndex: Long): Option[ResponseState] = {
    val JournalBounds(min,max) = minMax
    if (requestedFromLogIndex + 1 >= min && requestedFromLogIndex <= max) {
      val committed = (requestedFromLogIndex + 1) to committedLogIndex flatMap {
        journaled(_)
      }
      val proposed = (committedLogIndex + 1) to max flatMap {
        journaled(_)
      }
      Option(ResponseState(committed, proposed))
    } else {
      None
    }
  }
}