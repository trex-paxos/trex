package com.github.simbo1905.trex.library

/**
 * Tracks the responses to an accept message and when we timeout on getting a majority response
 * @param timeout The point in time we timeout.
 * @param accept The accept that we are awaiting responses.
 * @param responses The known responses.
 */
case class AcceptResponsesAndTimeout(timeout: Long, accept: Accept, responses: Map[Int, AcceptResponse])

trait AcceptResponsesHandler[ClientRef] extends PaxosLenses[ClientRef] {

  def plog: PaxosLogging

  def send(actor: ClientRef, msg: Any)

  def sendNoLongerLeader(clientCommands: Map[Identifier, (CommandValue, ClientRef)]): Unit

  def randomTimeout: Long

  def backdownData(data: PaxosData[ClientRef]): PaxosData[ClientRef]

  def commit(state: PaxosRole, data: PaxosData[ClientRef], identifier: Identifier, progress: Progress): (Progress, Seq[(Identifier, Any)])

  def journalProgress(progress: Progress): Progress

  def broadcast(msg: Any): Unit

  def handleAcceptResponse(nodeUniqueId: Int, stateName: PaxosRole, sender: ClientRef, vote: AcceptResponse, oldData: PaxosData[ClientRef]): (PaxosRole, PaxosData[ClientRef]) = {
    val highestCommitted = oldData.progress.highestCommitted.logIndex
    val highestCommittedOther = vote.progress.highestCommitted.logIndex
    highestCommitted < highestCommittedOther match {
      case true =>
        (Follower,backdownData(oldData))
      case false =>
        oldData.acceptResponses.get(vote.requestId) match {
          case Some(AcceptResponsesAndTimeout(_, accept, votes)) =>
            val latestVotes = votes + (vote.from -> vote)
            val (positives, negatives) = latestVotes.toList.partition(_._2.isInstanceOf[AcceptAck])
            if (negatives.size > oldData.clusterSize / 2) {
              plog.info("Node {} {} received a majority accept nack so has lost leadership becoming a follower.", nodeUniqueId, stateName)
              sendNoLongerLeader(oldData.clientCommands)
              (Follower,backdownData(oldData))
            } else if (positives.size > oldData.clusterSize / 2) {
              // this slot is fixed record that we are not awaiting any more votes
              val updated = oldData.acceptResponses + (vote.requestId -> AcceptResponsesAndTimeout(randomTimeout, accept, Map.empty))

              // grab all the accepted values from the beginning of the tree map
              val (committable, uncommittable) = updated.span { case (_, AcceptResponsesAndTimeout(_, _, rs)) => rs.isEmpty }
              plog.debug("Node " + nodeUniqueId + " {} vote {} committable {} uncommittable {}", stateName, vote, committable, uncommittable)

              // this will have dropped the committable
              val votesData = acceptResponsesLens.set(oldData, uncommittable)

              // attempt an in-sequence commit
              committable.headOption match {
                case None =>
                  (stateName, votesData) // gap in committable sequence
                case Some((id,_)) if id.logIndex != votesData.progress.highestCommitted.logIndex + 1 =>
                  plog.error(s"Node $nodeUniqueId $stateName invariant violation: $stateName has committable work which is not contiguous with progress implying we have not issued Prepare/Accept messages for the correct range of slots. Returning to follower.")
                  sendNoLongerLeader(oldData.clientCommands)
                  (Follower,backdownData(oldData))
                case _ =>
                  val (newProgress, results) = commit(stateName, oldData, committable.last._1, votesData.progress)

                  journalProgress(newProgress)

                  broadcast(Commit(newProgress.highestCommitted))

                  if (oldData.clientCommands.nonEmpty) {
                    val (committedIds, _) = results.unzip

                    // TODO do the tests check that we clear out the responses which match the work we have committed?
                    val (responds, remainders) = oldData.clientCommands.partition {
                      idCmdRef: (Identifier, (CommandValue, ClientRef)) =>
                        val (id, (_, _)) = idCmdRef
                        committedIds.contains(id)
                    }

                    plog.debug("Node {} {} post commit has responds.size={}, remainders.size={}", nodeUniqueId, stateName, responds.size, remainders.size)
                    results foreach { case (id, bytes) =>
                      responds.get(id) foreach { case (cmd, client) =>
                        plog.debug("sending response from accept {} to {}", id, client)
                        send(client, bytes)
                      }
                    }
                    (stateName, progressLens.set(votesData, newProgress).copy(clientCommands = remainders))// TODO new lens?
                  } else {
                    (stateName, progressLens.set(votesData, newProgress))
                  }
              }
            } else {
              // insufficient votes keep counting
              val updated = oldData.acceptResponses + (vote.requestId -> AcceptResponsesAndTimeout(randomTimeout, accept, latestVotes))
              plog.debug("Node {} {} insufficient votes for {} have {}", nodeUniqueId, stateName, vote.requestId, updated)
              (stateName, acceptResponsesLens.set(oldData, updated))
            }
          case None =>
            plog.debug("Node {} {} ignoring response we are not awaiting: {}", nodeUniqueId, stateName, vote)
            (stateName, oldData)
        }
    }
  }
}
