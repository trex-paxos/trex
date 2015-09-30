package com.github.simbo1905.trex.library

import scala.collection.immutable.SortedMap

import Ordering._

trait PrepareResponseHandler[ClientRef] extends PaxosLenses[ClientRef] with BackdownData[ClientRef] {

  def requestRetransmissionIfBehind(io: PaxosIO[ClientRef], agent: PaxosAgent[ClientRef], from: Int, highestCommitted: Identifier): Unit = {
    val highestCommittedIndex = agent.data.progress.highestCommitted.logIndex
    val highestCommittedIndexOther = highestCommitted.logIndex
    if (highestCommittedIndexOther > highestCommittedIndex) {
      io.plog.info("Node {} Recoverer requesting retransmission to target {} with highestCommittedIndex {}", agent.nodeUniqueId, from, highestCommittedIndex)
      io.send( RetransmitRequest(agent.nodeUniqueId, from, highestCommittedIndex))
    }
  }

  def handlePrepareResponse(io: PaxosIO[ClientRef], agent: PaxosAgent[ClientRef], vote: PrepareResponse): PaxosAgent[ClientRef] = {
    require(agent.role == Recoverer, s"handle prepare response must be called in state Recoverer not ${agent.role}")
    requestRetransmissionIfBehind(io, agent, vote.from, vote.progress.highestCommitted)

    val id = vote.requestId

    agent.data.prepareResponses.getOrElse(id, Map.empty) match {
      case map if map.isEmpty =>
        // we already had a majority positive response so nothing to do
        io.plog.debug("Node {} Ignored prepare response as no longer tracking this request: {}", agent.nodeUniqueId, vote)
        agent
      case map =>
        // register the vote
        val votes = map + (vote.from -> vote)

        // tally the votes
        val (positives, negatives) = votes.partition {
          case (_, response) => response.isInstanceOf[PrepareAck]
        }
        if (positives.size > agent.data.clusterSize / 2) {
          // issue new prepare messages if others have accepted higher slot indexes
          val dataWithExpandedPrepareResponses: PaxosData[ClientRef] = if (votes.size > agent.data.clusterSize / 2) {
            // issue more prepares there are more accepted slots than we so far ran recovery upon
            agent.data.prepareResponses.lastOption match {
              case Some((Identifier(_, _, ourLastHighestAccepted), _)) =>
                val theirHighestAccepted = votes.values.map(_.highestAcceptedIndex).max
                if (theirHighestAccepted > ourLastHighestAccepted) {
                  val prepares = (ourLastHighestAccepted + 1) to theirHighestAccepted map { id =>
                    Prepare(Identifier(agent.nodeUniqueId, agent.data.epoch.get, id))
                  }
                  io.plog.info("Node {} Recoverer broadcasting {} new prepare messages for expanded slots {} to {}", agent.nodeUniqueId, prepares.size, (ourLastHighestAccepted + 1), theirHighestAccepted)
                  prepares foreach { p =>
                    io.plog.debug("Node {} sending {}", agent.nodeUniqueId, p)
                    io.send(p)
                  }

                  // accept our own prepare if we have not made a higher promise
                  val newPrepareSelfVotes: SortedMap[Identifier, Map[Int, PrepareResponse]] =
                    (prepares map { prepare =>
                      val ackOrNack = if (prepare.id.number >= agent.data.progress.highestPromised) {
                        PrepareAck(prepare.id, agent.nodeUniqueId, agent.data.progress, ourLastHighestAccepted, agent.data.leaderHeartbeat, io.journal.accepted(prepare.id.logIndex))
                      } else {
                        PrepareNack(prepare.id, agent.nodeUniqueId, agent.data.progress, ourLastHighestAccepted, agent.data.leaderHeartbeat)
                      }
                      val selfVote = Map(agent.nodeUniqueId -> ackOrNack)
                      (prepare.id -> selfVote)
                    })(scala.collection.breakOut)
                  prepareResponsesLens.set(agent.data, agent.data.prepareResponses ++ newPrepareSelfVotes)
                } else {
                  agent.data
                }
              case None =>
                agent.data
            }
          } else {
            agent.data
          }
          // success gather any values
          val accepts = positives.values.map(_.asInstanceOf[PrepareAck]).flatMap(_.highestUncommitted)
          val accept = if (accepts.isEmpty) {
            val accept = Accept(id, NoOperationCommandValue)
            io.plog.info("Node {} {} got a majority of positive prepare response with no value sending fresh NO_OPERATION accept message {}", agent.nodeUniqueId, agent.role, accept)
            accept
          } else {
            val max = accepts.maxBy(_.id.number)
            val accept = Accept(id, max.value)
            io.plog.info("Node {} {} got a majority of positive prepare response with highest accept message {} sending fresh message {}", agent.nodeUniqueId, agent.role, max.id, accept)
            accept
          }
          // only accept your own broadcast if we have not made a higher promise whilst awaiting responses from other nodes
          val selfResponse: AcceptResponse = if (accept.id.number >= dataWithExpandedPrepareResponses.progress.highestPromised) {
            io.plog.debug("Node {} {} accepting own message {}", agent.nodeUniqueId, agent.role, accept.id)
            io.journal.accept(accept)
            AcceptAck(accept.id, agent.nodeUniqueId, dataWithExpandedPrepareResponses.progress)
          } else {
            io.plog.debug("Node {} {} not accepting own message with number {} as have made a higher promise {}", agent.nodeUniqueId, agent.role, accept.id.number, dataWithExpandedPrepareResponses.progress.highestPromised)
            AcceptNack(accept.id, agent.nodeUniqueId, dataWithExpandedPrepareResponses.progress)
          }
          // broadcast accept
          io.plog.debug("Node {} {} sending {}", agent.nodeUniqueId, agent.role, accept)
          io.send(accept)
          // create a fresh vote for your new accept message
          val selfVoted = dataWithExpandedPrepareResponses.acceptResponses + (accept.id -> AcceptResponsesAndTimeout(io.randomTimeout, accept, Map(agent.nodeUniqueId -> selfResponse)))
          // we are no longer awaiting responses to the prepare
          val expandedRecover = dataWithExpandedPrepareResponses.prepareResponses
          val updatedPrepares = expandedRecover - vote.requestId
          if (updatedPrepares.isEmpty) {
            // we have completed recovery of the values in the slots so we now switch to stable Leader state
            val newData = leaderLens.set(dataWithExpandedPrepareResponses, (SortedMap.empty, selfVoted, Map.empty))
            io.plog.info("Node {} {} has issued accept messages for all prepare messages to promoting to be Leader.", agent.nodeUniqueId, agent.role)
            agent.copy(role = Leader, data = newData.copy(clientCommands = Map.empty, timeout = io.randomTimeout)) // TODO lens?
          } else {
            io.plog.info("Node {} {} is still recovering {} slots", agent.nodeUniqueId, agent.role, updatedPrepares.size)
            agent.copy(data = leaderLens.set(dataWithExpandedPrepareResponses, (updatedPrepares, selfVoted, Map.empty)))
          }
        } else if (negatives.size > agent.data.clusterSize / 2) {
          io.plog.info("Node {} {} received {} prepare nacks returning to follower", agent.nodeUniqueId, agent.role, negatives.size)
          agent.copy(role = Follower, data = backdownData(io, agent.data))
          }
        else {
          // FIXME what happens if we have an even number of nodes and a split vote?
          val updated = agent.data.prepareResponses + (vote.requestId -> votes)
          agent.copy(data = prepareResponsesLens.set(agent.data, updated))
        }
    }
  }
}
