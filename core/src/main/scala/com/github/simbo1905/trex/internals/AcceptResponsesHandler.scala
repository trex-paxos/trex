package com.github.simbo1905.trex.internals

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import com.github.simbo1905.trex.Journal

trait AcceptResponsesHandler {

  def log: LoggingAdapter

  def send(actor: ActorRef, msg: Any)

  def sendNoLongerLeader(clientCommands: Map[Identifier, (CommandValue, ActorRef)]): Unit

  def randomTimeout: Long

  def backdownData(data: PaxosData): PaxosData

  def commit(state: PaxosRole, data: PaxosData, identifier: Identifier, progress: Progress): (Progress, Seq[(Identifier, Any)])

  def journal: Journal

  def broadcast(msg: Any): Unit

  def handleAcceptResponse(nodeUniqueId: Int, stateName: PaxosRole, sender: ActorRef, vote: AcceptResponse, oldData: PaxosData): (PaxosRole, PaxosData) = {

    oldData.acceptResponses.get(vote.requestId) match {
      case Some(AcceptResponsesAndTimeout(_, accept, votes)) =>
        val latestVotes = votes + (vote.from -> vote)
        if (latestVotes.size > oldData.clusterSize / 2) {
          // requestRetransmission if behind FIXME what if it cannot get a majority response will it get stuck not asking for retransmission?
          val highestCommittedIndex = oldData.progress.highestCommitted.logIndex
          val (target, highestCommittedIndexOther) = latestVotes.values.map(ar => ar.from -> ar.progress.highestCommitted.logIndex).maxBy(_._2)
          if (highestCommittedIndexOther > highestCommittedIndex) {
            log.debug("Node {} {} requesting retransmission to target {} with highestCommittedIndex {}", nodeUniqueId, stateName, target, highestCommittedIndex)
            send(sender, RetransmitRequest(nodeUniqueId, target, highestCommittedIndex))
          }
        }
        val (positives, negatives) = latestVotes.toList.partition(_._2.isInstanceOf[AcceptAck])
        if (negatives.size > oldData.clusterSize / 2) {
          log.info("Node {} {} received a majority accept nack so has lost leadership becoming a follower.", nodeUniqueId, stateName)
          sendNoLongerLeader(oldData.clientCommands)
          (Follower,backdownData(oldData))
        } else if (positives.size > oldData.clusterSize / 2) {
          // this slot is fixed record that we are not awaiting any more votes
          val updated = oldData.acceptResponses + (vote.requestId -> AcceptResponsesAndTimeout(randomTimeout, accept, Map.empty))

          // grab all the accepted values from the beginning of the tree map
          val (committable, uncommittable) = updated.span { case (_, AcceptResponsesAndTimeout(_, _, rs)) => rs.isEmpty }
          log.debug("Node " + nodeUniqueId + " {} vote {} committable {} uncommittable {}", stateName, vote, committable, uncommittable)

          // this will have dropped the committable
          val votesData = PaxosData.acceptResponsesLens.set(oldData, uncommittable)

          val x: Option[(Identifier, AcceptResponsesAndTimeout)] = committable.headOption

          // attempt an in-sequence commit
          committable.headOption match {
            case None =>
              (stateName, votesData) // gap in committable sequence
            case Some((id,_)) if id.logIndex != votesData.progress.highestCommitted.logIndex + 1 =>
              log.error(s"Node $nodeUniqueId $stateName invariant violation: $stateName has committable work which is not contiguous with progress implying we have not issued Prepare/Accept messages for the correct range of slots. Returning to follower.")
              sendNoLongerLeader(oldData.clientCommands)
              (Follower,backdownData(oldData))
            case _ =>
              val (newProgress, results) = commit(stateName, oldData, committable.last._1, votesData.progress)

              // FIXME test that the send of the commit happens after saving the progress
              // FIXME was no test checking that this was broadcast not just replied to sender
              broadcast(Commit(newProgress.highestCommitted))

              if (stateName == Leader && oldData.clientCommands.nonEmpty) {
                // TODO the nonEmpty guard is due to test data not setting this should fix the test data and remove it

                val (committedIds, _) = results.unzip

                val (responds, remainders) = oldData.clientCommands.partition {
                  idCmdRef: (Identifier, (CommandValue, ActorRef)) =>
                    val (id, (_, _)) = idCmdRef
                    committedIds.contains(id)
                }

                log.debug("Node {} {} post commit has responds.size={}, remainders.size={}", nodeUniqueId, stateName, responds.size, remainders.size)
                results foreach { case (id, bytes) =>
                  responds.get(id) foreach { case (cmd, client) =>
                    log.debug("sending response from accept {} to {}", id, client)
                    send(client, bytes)
                  }
                }
                // FIXME memory leak we have not GCed the stuff we are no longer awaiting responses in votesData.acceptResponses
                (stateName, PaxosData.progressLens.set(votesData, newProgress).copy(clientCommands = remainders))// TODO new lens?
              } else {
                (stateName, PaxosData.progressLens.set(votesData, newProgress))
              }
          }
        } else {
          // insufficient votes keep counting
          val updated = oldData.acceptResponses + (vote.requestId -> AcceptResponsesAndTimeout(randomTimeout, accept, latestVotes))
          log.debug("Node {} {} insufficent votes for {} have {}", nodeUniqueId, stateName, vote.requestId, updated)
          (stateName, PaxosData.acceptResponsesLens.set(oldData, updated))
        }
      case None =>
        log.debug("Node {} {} ignoring late response as saw a majority response: {}", nodeUniqueId, stateName, vote)
        (stateName, oldData)
    }
  }
}
