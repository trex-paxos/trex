package com.github.trex_paxos.library

/**
 * Tracks the responses to an accept message and when we timeout on getting a majority response
 * @param timeout The point in time we timeout.
 * @param accept The accept that we are awaiting responses.
 * @param responses The known responses.
 */
case class AcceptResponsesAndTimeout(timeout: Long, accept: Accept, responses: Map[Int, AcceptResponse])

trait AcceptResponseHandler extends PaxosLenses
with BackdownAgent
with CommitHandler {

  import AcceptResponseHandler._

  def handleAcceptResponse(io: PaxosIO, agent: PaxosAgent, vote: AcceptResponse): PaxosAgent = {
    io.logger.debug("{} sees response {}", agent.nodeUniqueId, vote)
    val highestCommitted = agent.data.progress.highestCommitted.logIndex
    val highestCommittedOther = vote.progress.highestCommitted.logIndex
    highestCommitted < highestCommittedOther match {
      case true =>
        backdownAgent(io, agent)
      case false =>
        agent.data.acceptResponses.get(vote.requestId) match {
          case Some(art@AcceptResponsesAndTimeout(_, accept, votes)) =>
            val latestVotes = votes + (vote.from -> vote)
            val freshResponse = latestVotes.size > votes.size // TODO this is O(N) consider votes.contains(response.from)

            freshResponse match {
              case false =>
                // ignore repeated response
                agent
              case true =>
                handleFreshResponse(io, agent, latestVotes, accept, vote)
            }
          case None =>
            io.logger.debug("Node {} {} ignoring response we are not awaiting: {}", agent.nodeUniqueId, agent.role, vote)
            agent
        }
    }
  }

  def handleFreshResponse(io: PaxosIO, agent: PaxosAgent, votes: Map[Int, AcceptResponse], accept: Accept, vote: AcceptResponse) = {

    agent.quorumStrategy.assessAccepts(votes.values) match {
      case Some(QuorumNack) =>
        io.logger.info("Node {} {} received a majority accept nack so has lost leadership becoming a follower.", agent.nodeUniqueId, agent.role)
        backdownAgent(io, agent)

      case Some(QuorumAck) =>
        // this slot is fixed record that we are not awaiting any more votes
        val updated = agent.data.acceptResponses + (vote.requestId -> AcceptResponsesAndTimeout(Long.MaxValue, accept, Map.empty))

        // grab all the accepted values from the beginning of the tree map
        val (committable, uncommittable) = updated.span { case (_, AcceptResponsesAndTimeout(_, _, rs)) => rs.isEmpty }
        io.logger.debug("Node " + agent.nodeUniqueId + " {} vote {} committable {} uncommittable {}", agent.role, vote, committable, uncommittable)

        // this will have dropped the committable
        val votesData = acceptResponsesLens.set(agent.data, uncommittable)

        // attempt an in-sequence commit
        committable.headOption match {
          case None =>
            agent.copy(data = votesData) // gap in committable sequence
          case Some((id, _)) if id.logIndex != votesData.progress.highestCommitted.logIndex + 1 =>
            io.logger.error(s"Node ${agent.nodeUniqueId} ${agent.role} invariant violation: ${agent.role} has committable work which is not contiguous with progress implying we have not issued Prepare/Accept messages for the correct range of slots. Returning to follower.")
            backdownAgent(io, agent)
          case _ =>
            // headOption isn't None so lastOption must be defined
            val (lastId, _) = committable.lastOption.getOrElse(unreachable)
            processCommit(io, agent.copy(data = votesData), lastId)
        }

      case Some(SplitVote) =>
        io.logger.info("Node {} {} got a split accept vote out of total returning to follower: {}", agent.nodeUniqueId, agent.role, votes)
        backdownAgent(io, agent)

      case None =>
        // insufficient votes keep counting
        val updated = agent.data.acceptResponses + (vote.requestId -> AcceptResponsesAndTimeout(io.scheduleRandomCheckTimeout, accept, votes))
        io.logger.debug("Node {} {} insufficient votes for {} have {}", agent.nodeUniqueId, agent.role, vote.requestId, updated)
        agent.copy(data = acceptResponsesLens.set(agent.data, updated))
    }
  }

  def processCommit(io: PaxosIO, agent: PaxosAgent, lastId: Identifier) = {
    val (newProgress, results) = commit(io, agent, lastId)

    io.logger.debug("Node {} committed {} with new progress {} and results {}", agent.nodeUniqueId, lastId, newProgress, results)

    // flushing progress to disk must always occur before sending any messages.
    io.journal.saveProgress(newProgress)

    // sending of commit notification must happen after the disk flush. io.send may defer by buffering to serialize and transit on a different thread.
    io.send(Commit(newProgress.highestCommitted))

    // responding to the client should happen after flushing the progress to disk to ensure all nodes eventually agree.
    // the sending of the response to the client may be reordered with respect to sending the commit by buffering and serialization of commit or resposne on different threads
    io.respond(Option(results.toMap))

    // update the in memory state so that we are ready for the next message to be processed.
    agent.copy(data = progressLens.set(agent.data, newProgress))
  }
}

object AcceptResponseHandler {
  def unreachable = throw new AssertionError("this code should be unreachable")

}