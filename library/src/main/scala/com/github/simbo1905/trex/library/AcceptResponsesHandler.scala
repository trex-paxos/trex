package com.github.simbo1905.trex.library

/**
 * Tracks the responses to an accept message and when we timeout on getting a majority response
 * @param timeout The point in time we timeout.
 * @param accept The accept that we are awaiting responses.
 * @param responses The known responses.
 */
case class AcceptResponsesAndTimeout(timeout: Long, accept: Accept, responses: Map[Int, AcceptResponse])

trait AcceptResponsesHandler[RemoteRef] extends PaxosLenses[RemoteRef]
with BackdownAgent[RemoteRef]
with CommitHandler[RemoteRef]{

  import AcceptResponsesHandler._

  def handleAcceptResponse(io: PaxosIO[RemoteRef], agent: PaxosAgent[RemoteRef], vote: AcceptResponse): PaxosAgent[RemoteRef] = {

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
            io.plog.debug("Node {} {} ignoring response we are not awaiting: {}", agent.nodeUniqueId, agent.role, vote)
            agent
        }
    }
  }

  def handleFreshResponse(io: PaxosIO[RemoteRef], agent: PaxosAgent[RemoteRef], votes: Map[Int, AcceptResponse], accept: Accept, vote: AcceptResponse) = {
    val (positives, negatives) = votes.toList.partition {
      case (_, v: AcceptAck) => true
      case _ => false
    }
    val majorityNegativeResponse = negatives.size > agent.data.clusterSize / 2
    lazy val majorityPositiveResponse = positives.size > agent.data.clusterSize / 2
    lazy val splitVote = !majorityPositiveResponse && !majorityNegativeResponse && votes.size == agent.data.clusterSize

    if (majorityNegativeResponse) {
      io.plog.info("Node {} {} received a majority accept nack so has lost leadership becoming a follower.", agent.nodeUniqueId, agent.role)
      backdownAgent(io, agent)
    } else if (majorityPositiveResponse) {
      // this slot is fixed record that we are not awaiting any more votes
      val updated = agent.data.acceptResponses + (vote.requestId -> AcceptResponsesAndTimeout(Long.MaxValue, accept, Map.empty))

      // grab all the accepted values from the beginning of the tree map
      val (committable, uncommittable) = updated.span { case (_, AcceptResponsesAndTimeout(_, _, rs)) => rs.isEmpty }
      io.plog.debug("Node " + agent.nodeUniqueId + " {} vote {} committable {} uncommittable {}", agent.role, vote, committable, uncommittable)

      // this will have dropped the committable
      val votesData = acceptResponsesLens.set(agent.data, uncommittable)

      // attempt an in-sequence commit
      committable.headOption match {
        case None =>
          agent.copy(data = votesData) // gap in committable sequence
        case Some((id, _)) if id.logIndex != votesData.progress.highestCommitted.logIndex + 1 =>
          io.plog.error(s"Node ${agent.nodeUniqueId} ${agent.role} invariant violation: ${agent.role} has committable work which is not contiguous with progress implying we have not issued Prepare/Accept messages for the correct range of slots. Returning to follower.")
          backdownAgent(io, agent)
        case _ =>
          // headOption isn't None so lastOption must be defined
          val (lastId, _) = committable.lastOption.getOrElse(unreachable)
          val (newProgress, results) = commit(io, agent, lastId)

          io.journal.save(newProgress)

          io.send(Commit(newProgress.highestCommitted))

          if (agent.data.clientCommands.nonEmpty) {
            val (multipleCommittedIds, _) = results.unzip

            // FIXME do the tests check that we clear out the responses which match the work we have committed?
            val (responds, remainders) = agent.data.clientCommands.partition {
              idCmdRef: (Identifier, (CommandValue, RemoteRef)) =>
                val (id, (_, _)) = idCmdRef
                multipleCommittedIds.contains(id)
            }

            io.plog.debug("Node {} {} post commit has responds.size={}, remainders.size={}", agent.nodeUniqueId, agent.role, responds.size, remainders.size)
            results foreach { case (id, bytes) =>
              responds.get(id) foreach { case (cmd, client) =>
                io.plog.debug("sending response from accept {} to {}", id, client)
                io.respond(client, bytes)
              }
            }
            agent.copy(data = progressLens.set(votesData, newProgress).copy(clientCommands = remainders))
          } else {
            agent.copy(data = progressLens.set(votesData, newProgress))
          }
      }
    } else if (splitVote) {
      io.plog.info("Node {} {} got a split accept vote out of total returning to follower: {}", agent.nodeUniqueId, agent.role, votes)
      backdownAgent(io, agent)
    } else {
      // insufficient votes keep counting
      val updated = agent.data.acceptResponses + (vote.requestId -> AcceptResponsesAndTimeout(io.randomTimeout, accept, votes))
      io.plog.debug("Node {} {} insufficient votes for {} have {}", agent.nodeUniqueId, agent.role, vote.requestId, updated)
      agent.copy(data = acceptResponsesLens.set(agent.data, updated))
    }
  }
}

object AcceptResponsesHandler {
  def unreachable = throw new AssertionError("this code should be unreachable")


}