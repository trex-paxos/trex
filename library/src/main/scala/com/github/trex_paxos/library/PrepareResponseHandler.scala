package com.github.trex_paxos.library

import Ordering._

import scala.collection.immutable.SortedMap

trait PrepareResponseHandler extends PaxosLenses with BackdownAgent {

  import PrepareResponseHandler._

  def requestRetransmissionIfBehind(io: PaxosIO, agent: PaxosAgent, from: Int, highestCommitted: Identifier): Unit = {
    val highestCommittedIndex = agent.data.progress.highestCommitted.logIndex
    val highestCommittedIndexOther = highestCommitted.logIndex
    if (highestCommittedIndexOther > highestCommittedIndex) {
      io.logger.info("Node {} Recoverer requesting retransmission to target {} with highestCommittedIndex {}", agent.nodeUniqueId, from, highestCommittedIndex)
      io.send(RetransmitRequest(agent.nodeUniqueId, from, highestCommittedIndex))
    }
  }

  def handlePrepareResponse(io: PaxosIO, agent: PaxosAgent, vote: PrepareResponse): PaxosAgent = {
    require(agent.role == Recoverer, s"handle prepare response must be called in state Recoverer not ${agent.role}")
    io.logger.debug("{} sees response {}", agent.nodeUniqueId, vote)
    requestRetransmissionIfBehind(io, agent, vote.from, vote.progress.highestCommitted)

    val id = vote.requestId

    agent.data.prepareResponses.getOrElse(id, Map.empty) match {
      case map if map.isEmpty =>
        // ignore late responses when we already had a majority so no longer waiting
        io.logger.debug("Node {} Ignored prepare response as not tracking this request: {}", agent.nodeUniqueId, vote)
        agent
      case map =>
        // register the vote
        val votes = map + (vote.from -> vote)

        agent.quorumStrategy.assessPromises(votes.values) match {
          case Some(QuorumAck) =>
            // issue new prepare messages if others have accepted higher slot indexes
            val expandedPreparesData = expandedPrepareSlotRange(io, this, agent, votes)
            // choose the value to set as the highest returned from a majority response else a noop
            val accept: Accept = chooseAccept(io, agent, votes.values, id)
            // only accept your own broadcast if we have not made a higher promise whilst awaiting responses from other nodes
            val selfResponse: AcceptResponse = respondToSelf(io, agent, agent.data, accept)
            // broadcast accept
            io.logger.debug("Node {} {} sending {}", agent.nodeUniqueId, agent.role, accept)
            io.send(accept)
            // create a fresh vote for your new accept message
            val expandedAccepts = agent.data.acceptResponses + (accept.id -> AcceptResponsesAndTimeout(io.randomTimeout, accept, Map(agent.nodeUniqueId -> selfResponse)))
            // we are no longer awaiting responses to the prepare
            val updatedPrepares = expandedPreparesData - vote.requestId
            val newData = leaderLens.set(agent.data, (updatedPrepares, expandedAccepts))
            if (updatedPrepares.isEmpty) {
              // we have completed recovery so we now switch to stable Leader state
              io.logger.info("Node {} {} has issued accept messages for all prepare messages to promoting to be Leader.", agent.nodeUniqueId, agent.role)
              agent.copy(role = Leader, data = newData.copy(timeout = io.randomTimeout))
            } else {
              io.logger.info("Node {} {} is still recovering {} slots", agent.nodeUniqueId, agent.role, updatedPrepares.size)
              agent.copy(data = newData)
            }

          case Some(QuorumNack) =>
            io.logger.info("Node {} {} received majority prepare nacks returning to follower", agent.nodeUniqueId, agent.role)
            backdownAgent(io, agent)

          case Some(SplitVote) =>
            io.logger.warning("Node {} {} got a split prepare vote returning to follower", agent.nodeUniqueId, agent.role)
            backdownAgent(io, agent)

          case None =>
            val updated = agent.data.prepareResponses + (vote.requestId -> votes)
            agent.copy(data = prepareResponsesLens.set(agent.data, updated))
        }
    }
  }
}

object PrepareResponseHandler {
  def expandedPrepareSlotRange(io: PaxosIO,
                                          lenses: PaxosLenses,
                                          agent: PaxosAgent,
                                          votes: Map[Int, PrepareResponse]): SortedMap[Identifier, Map[Int, PrepareResponse]] = {
    // issue more prepares there are more accepted slots than we so far ran recovery upon
    agent.data.prepareResponses.lastOption match {
      case Some((Identifier(_, _, highestKnownSlotToRecover), _)) =>
        val highestSlotToRecoverLatestResponse = votes.values.map(_.highestAcceptedIndex).max
        if (highestSlotToRecoverLatestResponse > highestKnownSlotToRecover) {
          val prepares = (highestKnownSlotToRecover + 1) to highestSlotToRecoverLatestResponse map { id =>
            Prepare(Identifier(agent.nodeUniqueId, agent.data.epoch.get, id))
          }
          io.logger.info("Node {} Recoverer broadcasting {} new prepare messages for expanded slots {} to {}", agent.nodeUniqueId, prepares.size, (highestKnownSlotToRecover + 1), highestSlotToRecoverLatestResponse)
          prepares foreach { p =>
            io.logger.debug("Node {} sending {}", agent.nodeUniqueId, p)
            io.send(p)
          }

          // accept our own prepare if we have not made a higher promise
          val newPrepareSelfVotes: SortedMap[Identifier, Map[Int, PrepareResponse]] =
            (prepares map { prepare =>
              val ackOrNack = if (prepare.id.number >= agent.data.progress.highestPromised) {
                PrepareAck(prepare.id, agent.nodeUniqueId, agent.data.progress, highestKnownSlotToRecover, agent.data.leaderHeartbeat, io.journal.accepted(prepare.id.logIndex))
              } else {
                PrepareNack(prepare.id, agent.nodeUniqueId, agent.data.progress, highestKnownSlotToRecover, agent.data.leaderHeartbeat)
              }
              val selfVote = Map(agent.nodeUniqueId -> ackOrNack)
              (prepare.id -> selfVote)
            }).to(SortedMap)
          agent.data.prepareResponses ++ newPrepareSelfVotes
        } else {
          // no additional slots learnt about from the responses
          agent.data.prepareResponses
        }
      case None =>
        // no longer awaiting any responses ignore a late response
        agent.data.prepareResponses
    }
  }

  def chooseAccept(io: PaxosIO, agent: PaxosAgent, positives: Iterable[PrepareResponse], id: Identifier) = {
    val accepts = positives flatMap {
      case PrepareAck(_, _, _, _, _, optionAccept) => optionAccept
      case _ => None
    }
    if (accepts.isEmpty) {
      val accept = Accept(id, NoOperationCommandValue)
      io.logger.info("Node {} {} got a majority of positive prepare response with no value sending fresh NO_OPERATION accept message {}", agent.nodeUniqueId, agent.role, accept)
      accept
    } else {
      val max = accepts.maxBy(_.id.number)
      val accept = Accept(id, max.value)
      io.logger.info("Node {} {} got a majority of positive prepare response with highest accept message {} sending fresh message {}", agent.nodeUniqueId, agent.role, max.id, accept)
      accept
    }
  }

  def respondToSelf(io: PaxosIO, agent: PaxosAgent, expandedData: PaxosData, accept: Accept) = {
    if (accept.id.number >= expandedData.progress.highestPromised) {
      io.logger.debug("Node {} {} accepting own message {}", agent.nodeUniqueId, agent.role, accept.id)
      io.journal.accept(accept)
      AcceptAck(accept.id, agent.nodeUniqueId, expandedData.progress)
    } else {
      io.logger.debug("Node {} {} not accepting own message with number {} as have made a higher promise {}", agent.nodeUniqueId, agent.role, accept.id.number, expandedData.progress.highestPromised)
      AcceptNack(accept.id, agent.nodeUniqueId, expandedData.progress)
    }
  }
}
