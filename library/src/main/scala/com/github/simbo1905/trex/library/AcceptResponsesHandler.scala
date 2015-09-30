package com.github.simbo1905.trex.library

/**
 * Tracks the responses to an accept message and when we timeout on getting a majority response
 * @param timeout The point in time we timeout.
 * @param accept The accept that we are awaiting responses.
 * @param responses The known responses.
 */
case class AcceptResponsesAndTimeout(timeout: Long, accept: Accept, responses: Map[Int, AcceptResponse])

trait AcceptResponsesHandler[ClientRef] extends PaxosLenses[ClientRef] with BackdownData[ClientRef] {

  def commit(io: PaxosIO[ClientRef], state: PaxosRole, data: PaxosData[ClientRef], identifier: Identifier, progress: Progress): (Progress, Seq[(Identifier, Any)])

  def handleAcceptResponse(io: PaxosIO[ClientRef], agent: PaxosAgent[ClientRef], vote: AcceptResponse): PaxosAgent[ClientRef] = {

    val highestCommitted = agent.data.progress.highestCommitted.logIndex
    val highestCommittedOther = vote.progress.highestCommitted.logIndex
    highestCommitted < highestCommittedOther match {
      case true =>
        PaxosAgent(agent.nodeUniqueId, Follower, backdownData(io, agent.data))
      case false =>
        agent.data.acceptResponses.get(vote.requestId) match {
          case Some(AcceptResponsesAndTimeout(_, accept, votes)) =>
            val latestVotes = votes + (vote.from -> vote)
            val (positives, negatives) = latestVotes.toList.partition(_._2.isInstanceOf[AcceptAck])
            if (negatives.size > agent.data.clusterSize / 2) {
              io.plog.info("Node {} {} received a majority accept nack so has lost leadership becoming a follower.", agent.nodeUniqueId, agent.role)
              PaxosAgent(agent.nodeUniqueId, Follower, backdownData(io, agent.data))
            } else if (positives.size > agent.data.clusterSize / 2) {
              // this slot is fixed record that we are not awaiting any more votes
              val updated = agent.data.acceptResponses + (vote.requestId -> AcceptResponsesAndTimeout(io.randomTimeout, accept, Map.empty))

              // grab all the accepted values from the beginning of the tree map
              val (committable, uncommittable) = updated.span { case (_, AcceptResponsesAndTimeout(_, _, rs)) => rs.isEmpty }
              io.plog.debug("Node " + agent.nodeUniqueId + " {} vote {} committable {} uncommittable {}", agent.role, vote, committable, uncommittable)

              // this will have dropped the committable
              val votesData = acceptResponsesLens.set(agent.data, uncommittable)

              // attempt an in-sequence commit
              committable.headOption match {
                case None =>
                  PaxosAgent(agent.nodeUniqueId, agent.role, votesData) // gap in committable sequence
                case Some((id, _)) if id.logIndex != votesData.progress.highestCommitted.logIndex + 1 =>
                  io.plog.error(s"Node ${agent.nodeUniqueId} ${agent.role} invariant violation: ${agent.role} has committable work which is not contiguous with progress implying we have not issued Prepare/Accept messages for the correct range of slots. Returning to follower.")
                  PaxosAgent(agent.nodeUniqueId, Follower, backdownData(io, agent.data))
                case _ =>
                  val (newProgress, results) = commit(io, agent.role, agent.data, committable.last._1, votesData.progress)

                  io.journal.save(newProgress)

                  io.send(Commit(newProgress.highestCommitted))

                  if (agent.data.clientCommands.nonEmpty) {
                    val (committedIds, _) = results.unzip

                    // FIXME do the tests check that we clear out the responses which match the work we have committed?
                    val (responds, remainders) = agent.data.clientCommands.partition {
                      idCmdRef: (Identifier, (CommandValue, ClientRef)) =>
                        val (id, (_, _)) = idCmdRef
                        committedIds.contains(id)
                    }

                    io.plog.debug("Node {} {} post commit has responds.size={}, remainders.size={}", agent.nodeUniqueId, agent.role, responds.size, remainders.size)
                    results foreach { case (id, bytes) =>
                      responds.get(id) foreach { case (cmd, client) =>
                        io.plog.debug("sending response from accept {} to {}", id, client)
                        io.respond(client, bytes)
                      }
                    }
                    PaxosAgent(agent.nodeUniqueId, agent.role, progressLens.set(votesData, newProgress).copy(clientCommands = remainders)) // TODO new lens?
                  } else {
                    PaxosAgent(agent.nodeUniqueId, agent.role, progressLens.set(votesData, newProgress))
                  }
              }
            } else {
              // insufficient votes keep counting
              val updated = agent.data.acceptResponses + (vote.requestId -> AcceptResponsesAndTimeout(io.randomTimeout, accept, latestVotes))
              io.plog.debug("Node {} {} insufficient votes for {} have {}", agent.nodeUniqueId, agent.role, vote.requestId, updated)
              PaxosAgent(agent.nodeUniqueId, agent.role, acceptResponsesLens.set(agent.data, updated))
            }
          case None =>
            io.plog.debug("Node {} {} ignoring response we are not awaiting: {}", agent.nodeUniqueId, agent.role, vote)
            PaxosAgent(agent.nodeUniqueId, agent.role, agent.data)
        }
    }
  }
}
