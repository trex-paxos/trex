package com.github.trex_paxos.library

import Ordering._

import scala.collection.immutable.{SortedMap, TreeMap}

trait FollowerHandler extends PaxosLenses with BackdownAgent {
  import FollowerHandler._

  def highestAcceptedIndex(io: PaxosIO): Long = io.journal.bounds.max

  def handleFollowerResendLowPrepares(io: PaxosIO, agent: PaxosAgent): PaxosAgent = {
    io.logger.debug("Node {} {} timed-out having already issued a low. rebroadcasting", agent.nodeUniqueId, agent.role)
    io.send(io.minPrepare)
    agent.copy(data = timeoutLens.set(agent.data, io.randomTimeout))
  }

  def handleFollowerTimeout(io: PaxosIO, agent: PaxosAgent): PaxosAgent = {
    agent.data.prepareResponses.isEmpty match {
      case true =>
        sendLowPrepares(io, agent)
      case false =>
        handleFollowerResendLowPrepares(io, agent)
    }
  }
  
  def sendLowPrepares(io: PaxosIO, agent: PaxosAgent): PaxosAgent = {
    io.logger.info("Node {} {} timed-out progress: {}", agent.nodeUniqueId, agent.role, agent.data.progress)
    // nack our own prepare
    val prepareSelfVotes = SortedMap.empty[Identifier, Map[Int, PrepareResponse]] ++
      Map(io.minPrepare.id -> Map(agent.nodeUniqueId -> PrepareNack(io.minPrepare.id, agent.nodeUniqueId, agent.data.progress, highestAcceptedIndex(io), agent.data.leaderHeartbeat)))
    io.send(io.minPrepare)
    PaxosAgent(agent.nodeUniqueId, Follower, timeoutPrepareResponsesLens.set(agent.data, (io.randomTimeout, prepareSelfVotes)))
  }

  def handelFollowerPrepareResponse(io: PaxosIO, agent: PaxosAgent, vote: PrepareResponse): PaxosAgent = {

    agent.data.prepareResponses.nonEmpty match {
      case true =>
        // having broadcast a low prepare if we see insufficient evidence of a leader in a majority response promote to recoverer
        handleLowPrepareResponse(io, agent, vote)
      case false =>
        // we may see a prepare response that we are not awaiting any more which we will ignore
        agent
    }
  }

  def handleLowPrepareResponse(io: PaxosIO, agent: PaxosAgent, vote: PrepareResponse): PaxosAgent = {
    val selfHighestSlot = agent.data.progress.highestCommitted.logIndex
    val otherHighestSlot = vote.progress.highestCommitted.logIndex
    if (otherHighestSlot > selfHighestSlot) {
      io.logger.debug("Node {} node {} committed slot {} requesting retransmission", agent.nodeUniqueId, vote.from, otherHighestSlot)
      io.send(RetransmitRequest(agent.nodeUniqueId, vote.from, agent.data.progress.highestCommitted.logIndex))
      backdownAgent(io, agent)
    } else {
      agent.data.prepareResponses.get(vote.requestId) match {
        case Some(map) =>
          val votes = map + (vote.from -> vote)

          val haveMajorityResponse = votes.size > agent.data.clusterSize / 2

          if (haveMajorityResponse) {

            handleMajorityResponse(io, agent, votes)

          } else {
            // need to wait until we hear from a majority
            agent.copy(role = Follower, data = agent.data.copy(prepareResponses = TreeMap(Map(io.minPrepare.id -> votes).toArray: _*))) // TODO lens
          }
        case x =>
          io.logger.debug("Node {} {} is no longer awaiting responses to {} so ignoring {}", agent.nodeUniqueId, agent.role, vote.requestId, x)
          agent.copy(role = Follower)
      }
    }
  }

  def handleMajorityResponse(io: PaxosIO, agent: PaxosAgent, votes: Map[Int, PrepareResponse]): PaxosAgent = {
    computeFailover(io.logger, agent.nodeUniqueId, agent.data, votes) match {
      case FailoverResult(failover, _) if failover =>
        val highestNumber = Seq(agent.data.progress.highestPromised, agent.data.progress.highestCommitted.number).max
        val maxCommittedSlot = agent.data.progress.highestCommitted.logIndex
        // TODO issue #13 we ignore higher slots in responses here until we see accept responses we can speed up recovery by issuing more accepts
        val maxAcceptedSlot = highestAcceptedIndex(io)
        // create prepares for the known uncommitted slots else a refresh prepare for the next higher slot than committed
        val prepares = recoverPrepares(agent.nodeUniqueId, highestNumber, maxCommittedSlot, maxAcceptedSlot)
        // make a promise to self not to accept higher numbered messages and journal that
        prepares.headOption match {
          case Some(p) =>
            io.logger.info(s"Node {} {} is promoting to Recoverer and issuing high prepares. ", agent.nodeUniqueId, agent.role)
            val selfPromise = p.id.number
            // accept our own promise and load from the journal any values previous accepted in those slots
            val prepareSelfVotes: SortedMap[Identifier, Map[Int, PrepareResponse]] =
              (prepares map { prepare =>
                val selfVote = Map(agent.nodeUniqueId -> PrepareAck(prepare.id, agent.nodeUniqueId, agent.data.progress, highestAcceptedIndex(io), agent.data.leaderHeartbeat, io.journal.accepted(prepare.id.logIndex)))
                prepare.id -> selfVote
              })(scala.collection.breakOut)

            // the new leader epoch is the promise it made to itself
            val epoch: Option[BallotNumber] = Some(selfPromise)
            io.logger.info("Node {} Follower broadcast {} prepare messages with {} transitioning Recoverer max slot index {}.", agent.nodeUniqueId, prepares.size, selfPromise, maxAcceptedSlot)
            // make a promise to self not to accept higher numbered messages and journal that
            val newData = highestPromisedTimeoutEpochPrepareResponsesAcceptResponseLens.set(agent.data, (selfPromise, io.randomTimeout, epoch, prepareSelfVotes, SortedMap.empty))
            io.journal.save(newData.progress)
            prepares.foreach(io.send(_))
            agent.copy(role = Recoverer,
              data = newData)
          case None =>
            io.logger.error("this code should be unreachable")
            agent.copy(role = Follower, data = agent.data)
        }
      case FailoverResult(_, maxHeartbeat) =>
        io.logger.debug(s"Node {} {} sees evidence of a leader is not failing over. ", agent.nodeUniqueId, agent.role)
        // other nodes are showing a leader behind a partial network partition so we backdown.
        // we update the local known heartbeat in case that leader dies causing a new scenario were only this node can form a majority.
        val a@PaxosAgent(_, _, data) = backdownAgent(io, agent)
        a.copy(data = data.copy(leaderHeartbeat = maxHeartbeat))
      case x => throw new AssertionError(s"unreachable code $x")
    }
  }
}

case class FailoverResult(failover: Boolean, maxHeartbeat: Long)

object FollowerHandler {

  /**
   * Generates fresh prepare messages targeting the range of slots from the highest committed to one higher than the highest accepted slot positions.
   * @param highest Highest number known to this node.
   * @param highestCommittedIndex Highest slot committed at this node.
   * @param highestAcceptedIndex Highest slot where a value has been accepted by this node.
   */
  def recoverPrepares(nodeUniqueId: Int, highest: BallotNumber, highestCommittedIndex: Long, highestAcceptedIndex: Long) = {
    val BallotNumber(counter, _) = highest
    val higherNumber = BallotNumber(counter + 1, nodeUniqueId)
    val prepares = (highestCommittedIndex + 1) to (highestAcceptedIndex + 1) map {
      slot => Prepare(Identifier(nodeUniqueId, higherNumber, slot))
    }
    if (prepares.nonEmpty) prepares else Seq(Prepare(Identifier(nodeUniqueId, higherNumber, highestCommittedIndex + 1)))
  }

  def computeFailover(log: PaxosLogging, nodeUniqueId: Int, data: PaxosData, votes: Map[Int, PrepareResponse]): FailoverResult = {

    val largerHeartbeats: Iterable[Long] = votes.values flatMap {
      case PrepareNack(_, _, _, _, evidenceHeartbeat) if evidenceHeartbeat > data.leaderHeartbeat =>
        Some(evidenceHeartbeat)
      case _ =>
        None
    }

    lazy val largerHeartbeatCount = largerHeartbeats.size

    // here the plus one is to count the leader behind a network partition. so
    // in a three node cluster if our link to the leader goes down we will see
    // one heartbeat from the follower we can see and that plus the leader we
    // cannot see is the majority evidence of a leader behind a network partition
    def sufficientHeartbeatEvidence = largerHeartbeatCount + 1 > data.clusterSize / 2

    def noLargerHeartbeatEvidence = largerHeartbeats.isEmpty

    val decision = if (noLargerHeartbeatEvidence) {
      // all clear the last leader must be dead take over the leadership
      log.info("Node {} Follower no heartbeats executing takeover protocol.", nodeUniqueId)
      true
    } else if (sufficientHeartbeatEvidence) {
      // no need to failover as there is sufficient evidence to deduce that there is a leader which can contact a working majority
      log.info("Node {} Follower sees {} fresh heartbeats *not* execute the leader takeover protocol.", nodeUniqueId, largerHeartbeatCount)
      false
    } else {
      // insufficient evidence. this would be due to a complex network partition. if we don't attempt a
      // leader fail-over the cluster may halt. if we do we risk a leader duel. a duel is the lesser evil as you
      // can solve it by stopping a node until you heal the network partition(s). in the future the leader
      // may heartbeat at commit noop values probably when we have implemented the strong read
      // optimisation which will also prevent a duel.
      log.info("Node {} Follower sees {} heartbeats executing takeover protocol.",
        nodeUniqueId, largerHeartbeatCount)
      true
    }
    val allHeartbeats = largerHeartbeats ++ Seq(data.leaderHeartbeat)
    FailoverResult(decision, allHeartbeats.max)
  }
}