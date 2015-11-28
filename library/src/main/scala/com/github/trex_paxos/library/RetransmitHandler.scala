package com.github.trex_paxos.library

case class Retransmission(newProgress: Progress, accepts: Seq[Accept], committed: Seq[CommandValue])

trait RetransmitHandler extends PaxosLenses {

  import RetransmitHandler._

  def handleRetransmitResponse(io:PaxosIO, agent: PaxosAgent, response: RetransmitResponse): PaxosAgent = {
    // pure functional computation
    val retransmission = processRetransmitResponse(io, agent, response)

    // crash safety relies upon the following side effects happening in the correct order
    retransmission.committed.filter(_ != NoOperationCommandValue).foreach(io.deliver)

    io.journal.save(retransmission.newProgress)
    io.journal.accept(retransmission.accepts: _*)

    agent.copy(data = progressLens.set(agent.data, retransmission.newProgress))
  }

  /**
   * Computes the contiguous committable messages, all messages that may be accepted and the corresponding new progress.
   * If the accept messages are not in log index order they will not be processed which is a bug on send which will
   * halt the progress of the receiving node.  
   * @return
   */
  def processRetransmitResponse(io:PaxosIO, agent: PaxosAgent, response: RetransmitResponse): Retransmission = {
    val highestCommitted = agent.data.progress.highestCommitted
    val highestPromised = agent.data.progress.highestPromised

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
    val newProgress = Progress.highestPromisedHighestCommitted.set(agent.data.progress, (acceptState.highest, commitState.highestCommitted))

    io.logger.info("Node " + agent.nodeUniqueId + " RetransmitResponse committed {} of {} and accepted {} of {}", committedCount, aboveCommitted.size, acceptState.acceptable.size, response.uncommitted.size)

    // return new progress, the accepts that we should journal so that we may retransmit on request, and the committed values
    Retransmission(newProgress, (aboveCommitted ++ acceptState.acceptable).distinct, commitState.committed.map(_.value))
  }

  def handleRetransmitRequest(io: PaxosIO, agent: PaxosAgent, request: RetransmitRequest): PaxosAgent = {
    // extract who to respond to, where they are requesting from and where we are committed up to
    val RetransmitRequest(to, _, requestedLogIndex) = request
    val HighestCommittedIndex(committedLogIndex) = agent.data

    // send the response based on what we have in our journal, access to the journal, and where they have committed
    val responseData = processRetransmitRequest(io.journal.bounds, committedLogIndex, io.journal.accepted _, requestedLogIndex) map {
      case ResponseState(committed, uncommitted) =>
        RetransmitResponse(agent.nodeUniqueId, to, committed, uncommitted)
    }

    responseData foreach { r =>
      io.logger.info(s"Node ${agent.nodeUniqueId} retransmission response to node {} for logIndex {} with {} committed and {} proposed entries", r.from, request.logIndex, r.committed.size, r.uncommitted.size)
      io.send(r)
    }

    agent
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