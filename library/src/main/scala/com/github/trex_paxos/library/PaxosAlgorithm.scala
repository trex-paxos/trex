package com.github.trex_paxos.library

/**
  * A node in a paxos cluster
  * @param nodeUniqueId The node unique ID used in the ballot numbers. Assumed to never be recycled.
  * @param role The current role such as Follower or Leader
  * @param data The current state of the node holding the paxos algorithm bookwork
  * @param quorumStrategy The current quorum strategy (which could be any FPaxos flexible paxos strategy)
  */
case class PaxosAgent(nodeUniqueId: Int, role: PaxosRole, data: PaxosData, quorumStrategy: QuorumStrategy) {
  def minPrepare: Prepare = Prepare(Identifier(nodeUniqueId, BallotNumber(0, 0), 0))
}

/**
  * The latest event is an IO to read and write data (side effects), the paxos node, and a paxos message.
  * @param io
  * @param agent
  * @param message
  */
case class PaxosEvent(io: PaxosIO, agent: PaxosAgent, message: PaxosMessage)

/**
  * The result of processing an event.
  * @param agent The new agent state
  * @param messages The message to pass within the cluster
  */
case class PaxosResult(agent: PaxosAgent, messages: Seq[PaxosMessage])

/**
  * Paxos has side effects (writes to the network and read+write to disk) which are isolated into this trait to simplify testing.
  */
trait PaxosIO {

  /** The durable store to hold the state on disk.
    */
  def journal: Journal

  /**
    * A logging adaptor.
    */
  def logger: PaxosLogging

  /**
    * Schedule a future CheckTimeout PaxosMessage after a randomised interval.
    * @return The local clock wall time at which the timeout has occurred.
    */
  def scheduleRandomCheckTimeout: Long

  /**
    * The current time (we use a def so that we can override when testing).
    */
  def clock: Long

  /**
    * The callback to the host application which can side effect.
    * @param payload The fixed payload containing the byte array encoded client command value.
    * @return A byte array encoding of the host applications return value
    */
  def deliver(payload: Payload): Array[Byte]

  /**
    * Send a paxos algorithm message within the cluster. May be deferred.
    */
  def send(msg: PaxosMessage)

  /**
    * Associate a command value with a paxos identifier so that the result of the commit can be routed back to the sender of the command
    * @param value The command to asssociate with a message identifer.
    * @param id The message identifier
    */
  def associate(value: CommandValue, id: Identifier): Unit

  /**
    * Respond to clients.
    * @param results The results of a fast forward commit or None if leadership was lost such that the commit outcome is unknown.
    */
  def respond(results: Option[scala.collection.immutable.Map[Identifier, Any]]): Unit
}

object PaxosAlgorithm {
  type PaxosFunction = PartialFunction[PaxosEvent, PaxosAgent]
}

class PaxosAlgorithm extends PaxosLenses
  with CommitHandler
  with FollowerHandler
  with RetransmitHandler
  with PrepareHandler
  with AcceptHandler
  with PrepareResponseHandler
  with AcceptResponseHandler
  with ResendHandler
  with ReturnToFollowerHandler
  with ClientCommandHandler {

  import PaxosAlgorithm._

  val followingFunction: PaxosFunction = {
    // update heartbeat and attempt to commit contiguous accept messages
    case PaxosEvent(io, agent@PaxosAgent(_, Follower, _, _), c@Commit(i, heartbeat)) =>
      handleFollowerCommit(io, agent, c)
    case PaxosEvent(io, agent@PaxosAgent(_, Follower, PaxosData(_, _, to, _, _, _), _), CheckTimeout) if io.clock >= to =>
      handleFollowerTimeout(io, agent)
    case PaxosEvent(io, agent, vote: PrepareResponse) if agent.role == Follower =>
      handleFollowerPrepareResponse(io, agent, vote)
    // ignore an accept response which may be seen after we backdown to follower
    case PaxosEvent(_, agent@PaxosAgent(_, Follower, _, _), vote: AcceptResponse) =>
      agent
  }

  val retransmissionStateFunction: PaxosFunction = {
    case PaxosEvent(io, agent, rq: RetransmitRequest) =>
      handleRetransmitRequest(io, agent, rq)

    case PaxosEvent(io, agent, rs: RetransmitResponse) =>
      handleRetransmitResponse(io, agent, rs)
  }

  val prepareStateFunction: PaxosFunction = {
    case PaxosEvent(io, agent, p@Prepare(id)) =>
      handlePrepare(io, agent, p)
  }

  val acceptStateFunction: PaxosFunction = {
    case PaxosEvent(io, agent, a: Accept) =>
      handleAccept(io, agent, a)
  }

  val ignoreHeartbeatStateFunction: PaxosFunction = {
    // ingore a HeartBeat which has not already been handled
    case PaxosEvent(io, agent, HeartBeat) =>
      agent
  }

  val unknown: PaxosFunction = {
    case PaxosEvent(io, agent, x) =>
      io.logger.warning("unknown message {}", x)
      agent
  }

  /**
    * If no other logic has caught a timeout then do nothing.
    */
  val ignoreCheckTimeout: PaxosFunction = {
    case PaxosEvent(_, agent, CheckTimeout) =>
      agent
  }

  val lastFunction: PaxosFunction =
    acceptStateFunction orElse
      prepareStateFunction orElse
      retransmissionStateFunction orElse
      ignoreCheckTimeout orElse
      unknown

  val rejectCommandFunction: PaxosFunction = {
    case PaxosEvent(io, agent, v: CommandValue) =>
      io.send(NotLeader(agent.nodeUniqueId, v.msgUuid))
      agent
  }

  val notLeaderFunction: PaxosFunction = ignoreHeartbeatStateFunction orElse rejectCommandFunction

  val followerFunction: PaxosFunction = notLeaderFunction orElse followingFunction orElse lastFunction

  val takeoverFunction: PaxosFunction = {
    case PaxosEvent(io, agent, vote: PrepareResponse) =>
      handlePrepareResponse(io, agent, vote)
  }

  val acceptResponseFunction: PaxosFunction = {
    case PaxosEvent(io, agent, vote: AcceptResponse) =>
      handleAcceptResponse(io, agent, vote)
  }

  /**
    * Here on a timeout we deal with either pending prepares or pending accepts putting a priority on prepare handling
    * which backs down easily. Only if we have dealt with all timed out prepares do we handle timed out accepts which
    * is more aggressive as it attempts to go-higher than any other node number.
    */
  val resendPreparesAndAcceptsFunction: PaxosFunction = {
    // if we have timed-out on prepare messages
    case PaxosEvent(io, agent, CheckTimeout) if agent.data.prepareResponses.nonEmpty && io.clock > agent.data.timeout =>
      handleResendPrepares(io, agent, io.clock)

    // if we have timed-out on accept messages
    case PaxosEvent(io, agent, CheckTimeout) if agent.data.acceptResponses.nonEmpty && io.clock >= agent.data.timeout =>
      handleResendAccepts(io, agent, io.clock)
  }

  val backDownOnHigherCommit: PaxosFunction = {
    case PaxosEvent(io, agent, c: Commit) =>
      handleReturnToFollowerOnHigherCommit(io, agent, c)
  }

  val recoveringFunction: PaxosFunction =
    takeoverFunction orElse
      acceptResponseFunction orElse
      resendPreparesAndAcceptsFunction orElse
      backDownOnHigherCommit

  val recovererFunction: PaxosFunction = notLeaderFunction orElse recoveringFunction orElse lastFunction

  val leadingFunction: PaxosFunction = {
    // heartbeats the highest commit message
    case PaxosEvent(io, agent, HeartBeat) =>
      io.send(Commit(agent.data.progress.highestCommitted))
      agent

    // broadcasts a new client value
    case PaxosEvent(io, agent, value: CommandValue) =>
      handleClientCommand(io, agent, value)

    // ignore late vote as we would have transitioned on a majority ack
    case PaxosEvent(io, agent, value: PrepareResponse) =>
      agent
  }

  val leaderFunction: PaxosFunction =
    leadingFunction orElse
      acceptResponseFunction orElse
      resendPreparesAndAcceptsFunction orElse
      backDownOnHigherCommit orElse
      lastFunction

  def apply(e: PaxosEvent): PaxosAgent = e.agent.role match {
    case Follower => followerFunction(e)
    case Recoverer => recovererFunction(e)
    case Leader => leaderFunction(e)
  }
}
