package com.github.simbo1905.trex.library

import scala.collection.immutable.SortedMap

import Ordering._

trait PrepareResponseHandler[ClientRef] extends PaxosLenses[ClientRef] {

  def requestRetransmissionIfBehind(data: PaxosData[ClientRef], sender: ClientRef, from: Int, highestCommitted: Identifier): Unit

  def plog: PaxosLogging

  def broadcast(msg: Any): Unit

  def randomTimeout: Long

  def journal: Journal

  def backdownData(data: PaxosData[ClientRef]): PaxosData[ClientRef]

  def handlePrepareResponse(nodeUniqueId: Int, stateName: PaxosRole, sender: ClientRef, vote: PrepareResponse, data: PaxosData[ClientRef]): (PaxosRole, PaxosData[ClientRef]) = {
    require(stateName == Recoverer, s"handle prepare response must be called in state Recoverer not $stateName")
    requestRetransmissionIfBehind(data, sender, vote.from, vote.progress.highestCommitted)

    val id = vote.requestId

    data.prepareResponses.getOrElse(id, Map.empty) match {
      case map if map.isEmpty =>
        // we already had a majority positive response so nothing to do
        plog.debug("Node {} Ignored prepare response as no longer tracking this request: {}", nodeUniqueId, vote)
        (stateName, data)
      case map =>
        // register the vote
        val votes = map + (vote.from -> vote)

        // tally the votes
        val (positives, negatives) = votes.partition {
          case (_, response) => response.isInstanceOf[PrepareAck]
        }
        if (positives.size > data.clusterSize / 2) {
          // issue new prepare messages if others have accepted higher slot indexes
          val dataWithExpandedPrepareResponses: PaxosData[ClientRef] = if (votes.size > data.clusterSize / 2) {
            // issue more prepares there are more accepted slots than we so far ran recovery upon
            data.prepareResponses.lastOption match {
              case Some((Identifier(_, _, ourLastHighestAccepted), _)) =>
                val theirHighestAccepted = votes.values.map(_.highestAcceptedIndex).max
                if (theirHighestAccepted > ourLastHighestAccepted) {
                  val prepares = (ourLastHighestAccepted + 1) to theirHighestAccepted map { id =>
                    Prepare(Identifier(nodeUniqueId, data.epoch.get, id))
                  }
                  plog.info("Node {} Recoverer broadcasting {} new prepare messages for expanded slots {} to {}", nodeUniqueId, prepares.size, (ourLastHighestAccepted + 1), theirHighestAccepted)
                  prepares foreach { p =>
                    plog.debug("Node {} sending {}", nodeUniqueId, p)
                    broadcast(p)
                  }

                  // accept our own prepare if we have not made a higher promise
                  val newPrepareSelfVotes: SortedMap[Identifier, Map[Int, PrepareResponse]] =
                    (prepares map { prepare =>
                      val ackOrNack = if (prepare.id.number >= data.progress.highestPromised) {
                        PrepareAck(prepare.id, nodeUniqueId, data.progress, ourLastHighestAccepted, data.leaderHeartbeat, journal.accepted(prepare.id.logIndex))
                      } else {
                        PrepareNack(prepare.id, nodeUniqueId, data.progress, ourLastHighestAccepted, data.leaderHeartbeat)
                      }
                      val selfVote = Map(nodeUniqueId -> ackOrNack)
                      (prepare.id -> selfVote)
                    })(scala.collection.breakOut)
                  prepareResponsesLens.set(data, data.prepareResponses ++ newPrepareSelfVotes)
                } else {
                  data
                }
              case None =>
                data
            }
          } else {
            data
          }
          // success gather any values
          val accepts = positives.values.map(_.asInstanceOf[PrepareAck]).flatMap(_.highestUncommitted)
          val accept = if (accepts.isEmpty) {
            val accept = Accept(id, NoOperationCommandValue)
            plog.info("Node {} {} got a majority of positive prepare response with no value sending fresh NO_OPERATION accept message {}", nodeUniqueId, stateName, accept)
            accept
          } else {
            val max = accepts.maxBy(_.id.number)
            val accept = Accept(id, max.value)
            plog.info("Node {} {} got a majority of positive prepare response with highest accept message {} sending fresh message {}", nodeUniqueId, stateName, max.id, accept)
            accept
          }
          // only accept your own broadcast if we have not made a higher promise whilst awaiting responses from other nodes
          val selfResponse: AcceptResponse = if (accept.id.number >= dataWithExpandedPrepareResponses.progress.highestPromised) {
            plog.debug("Node {} {} accepting own message {}", nodeUniqueId, stateName, accept.id)
            journal.accept(accept)
            AcceptAck(accept.id, nodeUniqueId, dataWithExpandedPrepareResponses.progress)
          } else {
            plog.debug("Node {} {} not accepting own message with number {} as have made a higher promise {}", nodeUniqueId, stateName, accept.id.number, dataWithExpandedPrepareResponses.progress.highestPromised)
            AcceptNack(accept.id, nodeUniqueId, dataWithExpandedPrepareResponses.progress)
          }
          // broadcast accept
          plog.debug("Node {} {} sending {}", nodeUniqueId, stateName, accept)
          broadcast(accept)
          // create a fresh vote for your new accept message
          val selfVoted = dataWithExpandedPrepareResponses.acceptResponses + (accept.id -> AcceptResponsesAndTimeout(randomTimeout, accept, Map(nodeUniqueId -> selfResponse)))
          // we are no longer awaiting responses to the prepare
          val expandedRecover = dataWithExpandedPrepareResponses.prepareResponses
          val updatedPrepares = expandedRecover - vote.requestId
          if (updatedPrepares.isEmpty) {
            // we have completed recovery of the values in the slots so we now switch to stable Leader state
            val newData = leaderLens.set(dataWithExpandedPrepareResponses, (SortedMap.empty, selfVoted, Map.empty))
            plog.info("Node {} {} has issued accept messages for all prepare messages to promoting to be Leader.", nodeUniqueId, stateName)
            (Leader, newData.copy(clientCommands = Map.empty, timeout = randomTimeout)) // TODO lens?
          } else {
            plog.info("Node {} {} is still recovering {} slots", nodeUniqueId, stateName, updatedPrepares.size)
            (stateName, leaderLens.set(dataWithExpandedPrepareResponses, (updatedPrepares, selfVoted, Map.empty)))
          }
        } else if (negatives.size > data.clusterSize / 2) {
          plog.info("Node {} {} received {} prepare nacks returning to follower", nodeUniqueId, stateName, negatives.size)
          (Follower, backdownData(data))
          }
        else {
          // TODO what happens if we have an even number of nodes and a split vote?
          val updated = data.prepareResponses + (vote.requestId -> votes)
          (stateName, prepareResponsesLens.set(data, updated))
        }
    }
  }
}
