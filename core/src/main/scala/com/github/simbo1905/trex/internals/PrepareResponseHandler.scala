package com.github.simbo1905.trex.internals

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import com.github.simbo1905.trex.Journal

import scala.collection.SortedMap

import Ordering._

trait PrepareResponseHandler {

  def requestRetransmissionIfBehind(data: PaxosData, sender: ActorRef, from: Int, highestCommitted: Identifier): Unit

  def log: LoggingAdapter

  def broadcast(msg: Any): Unit

  def randomTimeout: Long

  def journal: Journal

  def backdownData(data: PaxosData): PaxosData

  def handlePrepareResponse(nodeUniqueId: Int, stateName: PaxosRole, sender: ActorRef, vote: PrepareResponse, data: PaxosData): (PaxosRole, PaxosData) = {
    requestRetransmissionIfBehind(data, sender, vote.from, vote.progress.highestCommitted)

    val id = vote.requestId

    if (id.from != nodeUniqueId) {
      log.info("Node {} {} message with id {} is not for this node", nodeUniqueId, stateName, id)
      (stateName, data) // FIXME test for this and do we want to add such a guard to the accept response processor?
    } else {
      data.prepareResponses.getOrElse(id, Map.empty) match {
        case map if map.isEmpty  =>
          // we already had a majority positive response so nothing to do
          log.debug("Node {} Ignored prepare response as no longer tracking this request: {}", nodeUniqueId, vote)
          (stateName, data)
        case map =>
          // register the vote
          val votes = map + (vote.from -> vote)

          // FIXME we send more messages here then have a path below where we ignore the responses by backing down.
          // if we have a majority response which show more slots to recover issue new prepare messages
          val dataWithExpandedPrepareResponses: PaxosData = if (votes.size > data.clusterSize / 2) {
            // issue more prepares there are more accepted slots than we so far ran recovery upon
            data.prepareResponses.lastOption match {
              case Some((Identifier(_, _, ourLastHighestAccepted), _)) =>
                val theirHighestAccepted = votes.values.map(_.highestAcceptedIndex).max
                if (theirHighestAccepted > ourLastHighestAccepted) {
                  val prepares = (ourLastHighestAccepted + 1) to theirHighestAccepted map { id =>
                    Prepare(Identifier(nodeUniqueId, data.epoch.get, id))
                  }
                  log.info("Node {} Recoverer broadcasting {} new prepare messages for expanded slots {} to {}", nodeUniqueId, prepares.size, (ourLastHighestAccepted + 1), theirHighestAccepted)
                  prepares foreach { p =>
                    log.debug("Node {} sending {}", nodeUniqueId, p)
                    broadcast(p)
                  }

                  // accept our own prepare if we have not made a higher promise
                  val newPrepareSelfVotes: SortedMap[Identifier, Map[Int, PrepareResponse]] =
                    (prepares map { prepare =>
                      val ackOrNack = if (prepare.id.number >= data.progress.highestPromised) {
                        PrepareAck(prepare.id, nodeUniqueId, data.progress, ourLastHighestAccepted, data.leaderHeartbeat, journal.accepted(prepare.id.logIndex))
                      } else {
                        // FIXME no test for this
                        PrepareNack(prepare.id, nodeUniqueId, data.progress, ourLastHighestAccepted, data.leaderHeartbeat)
                      }
                      val selfVote = Map(nodeUniqueId -> ackOrNack)
                      (prepare.id -> selfVote)
                    })(scala.collection.breakOut)
                  // FIXME no test for this
                  PaxosData.prepareResponsesLens.set(data, data.prepareResponses ++ newPrepareSelfVotes)
                } else {
                  data
                }
              case None =>
                data
            }
          } else {
            data
          }
          // tally the votes
          val (positives, negatives) = votes.partition {
            case (_, response) => response.isInstanceOf[PrepareAck]
          }
          if (positives.size > data.clusterSize / 2) {
            // success gather any values
            val accepts = positives.values.map(_.asInstanceOf[PrepareAck]).flatMap(_.highestUncommitted)
            val accept = if (accepts.isEmpty) {
              val accept = Accept(id, NoOperationCommandValue)
              log.info("Node {} {} got a majority of positive prepare response with no value sending fresh NO_OPERATION accept message {}", nodeUniqueId, stateName, accept)
              accept
            } else {
              val max = accepts.maxBy(_.id.number)
              val accept = Accept(id, max.value)
              log.info("Node {} {} got a majority of positive prepare response with highest accept message {} sending fresh message {}", nodeUniqueId, stateName, max.id, accept)
              accept
            }
            // broadcast accept
            log.debug("Node {} {} sending {}", nodeUniqueId, stateName, accept)
            broadcast(accept)
            // only accept your own broadcast if we have not made a higher promise whilst awaiting responses from other nodes
            val selfResponse: AcceptResponse = if (accept.id.number >= dataWithExpandedPrepareResponses.progress.highestPromised) {
              // FIXME had the inequality wrong way around and Recoverer tests didn't catch it. Add a test to cover this.
              log.debug("Node {} {} accepting own message {}", nodeUniqueId, stateName, accept.id)
              journal.accept(accept)
              AcceptAck(accept.id, nodeUniqueId, dataWithExpandedPrepareResponses.progress)
            } else {
              // FIXME no test for this
              log.debug("Node {} {} not accepting own message with number {} as have made a higher promise {}", nodeUniqueId, stateName, accept.id.number, dataWithExpandedPrepareResponses.progress.highestPromised)
              AcceptNack(accept.id, nodeUniqueId, dataWithExpandedPrepareResponses.progress)
            }
            // create a fresh vote for your new accept message
            val selfVoted = dataWithExpandedPrepareResponses.acceptResponses + (accept.id -> AcceptResponsesAndTimeout(randomTimeout, accept, Map(nodeUniqueId -> selfResponse)))
            // we are no longer awaiting responses to the prepare
            val expandedRecover = dataWithExpandedPrepareResponses.prepareResponses
            val updatedPrepares = expandedRecover - vote.requestId
            if (updatedPrepares.isEmpty) {
              // we have completed recovery of the values in the slots so we now switch to stable Leader state
              val newData = PaxosData.leaderLens.set(dataWithExpandedPrepareResponses, (SortedMap.empty, selfVoted, Map.empty))
              log.info("Node {} {} has issued accept messages for all prepare messages to promoting to be Leader.", nodeUniqueId, stateName)
              (Leader, newData.copy(clientCommands = Map.empty, timeout = randomTimeout)) // TODO lens?
            } else {
              log.info("Node {} {} is still recovering {} slots", nodeUniqueId, stateName, updatedPrepares.size)
              (stateName, PaxosData.leaderLens.set(dataWithExpandedPrepareResponses, (updatedPrepares, selfVoted, Map.empty)))
            }
          } else if (negatives.size > data.clusterSize / 2) {
            log.info("Node {} {} received {} prepare nacks returning to follower", nodeUniqueId, stateName, negatives.size)
            // FIXME not test for this
            (Follower, backdownData(data))
          }
          else {
            val updated = data.prepareResponses + (vote.requestId -> votes)
            (stateName, PaxosData.prepareResponsesLens.set(dataWithExpandedPrepareResponses, updated))
          }
      }
    }
  }
}
