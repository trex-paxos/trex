package com.github.simbo1905.trex.library

case class PaxosAgent[RemoteRef](nodeUniqueId: Int, role: PaxosRole, data: PaxosData[RemoteRef])

case class PaxosEvent[RemoteRef](io: PaxosIO[RemoteRef], agent: PaxosAgent[RemoteRef], message: PaxosMessage)

trait PaxosIO[RemoteRef] {
  def journal: Journal

  def plog: PaxosLogging

  def randomTimeout: Long

  def clock: Long

  def deliver(value: CommandValue): Any

  def respond(client: RemoteRef, data: Any)

  def send(msg: PaxosMessage)

  def sendNoLongerLeader(clientCommands: Map[Identifier, (CommandValue, RemoteRef)]): Unit

  def minPrepare: Prepare

  def sender: RemoteRef
}

object HighestCommittedIndexAndEpoch {
  def unapply(data: PaxosData[_]) = data.epoch match {
    case Some(number) => Some(data.progress.highestCommitted.logIndex, number)
    case None => None
  }
}

object PaxosAlgorithm {
  type PaxosFunction[RemoteRef] = PartialFunction[PaxosEvent[RemoteRef], PaxosAgent[RemoteRef]]

}

class PaxosAlgorithm[RemoteRef] extends PaxosLenses[RemoteRef]
with CommitHandler[RemoteRef]
with FollowerTimeoutHandler[RemoteRef]
with RetransmitHandler[RemoteRef]
with PromiseHandler[RemoteRef]
with HighAcceptHandler[RemoteRef]
with PrepareResponseHandler[RemoteRef]
with AcceptResponsesHandler[RemoteRef]
with ResendHandler[RemoteRef]
with ReturnToFollowerHandler[RemoteRef]
with ClientCommandHandler[RemoteRef] {

  import PaxosAlgorithm._

  val followingFunction: PaxosFunction[RemoteRef] = {
    // update heartbeat and attempt to commit contiguous accept messages
    case PaxosEvent(io, agent@PaxosAgent(_, Follower, _), c@Commit(i, heartbeat)) =>
      handleCommit(io, agent, c)
    // upon timeout having not issued low prepares start the leader takeover protocol by issuing a min prepare
    case PaxosEvent(io, agent@PaxosAgent(_, Follower, PaxosData(_, _, to, _, prepareResponses, _, _, _)), CheckTimeout) if io.clock >= to && prepareResponses.isEmpty =>
      handleFollowerTimeout(io, agent)
    // on a timeout where we have issued a low prepare but not yet received a majority response we should rebroadcast the low prepare
    case PaxosEvent(io, agent@PaxosAgent(_, Follower, PaxosData(_, _, to, _, prepareResponses, _, _, _)), CheckTimeout) if io.clock >= to && prepareResponses.nonEmpty =>
      handleResendLowPrepares(io, agent)
    // having broadcast a low prepare if we see insufficient evidence of a leader in a majority response promote to recoverer
    case PaxosEvent(io, agent@PaxosAgent(_, Follower, PaxosData(_, _, _, _, prepareResponses, _, _, _)), vote: PrepareResponse) if prepareResponses.nonEmpty =>
      handleLowPrepareResponse(io, agent, vote)
    // we may see a prepare response that we are not awaiting any more which we will ignore
    case PaxosEvent(_, agent@PaxosAgent(_, Follower, _), vote: PrepareResponse) if agent.data.prepareResponses.isEmpty => agent
    // ignore an accept response which may be seen after we backdown to follower
    case PaxosEvent(_, agent@PaxosAgent(_, Follower, _), vote: AcceptResponse) => agent
  }

  val retransmissionStateFunction: PaxosFunction[RemoteRef] = {
    case PaxosEvent(io, agent, rq: RetransmitRequest) =>
      handleRetransmitRequest(io, agent, rq)

    case PaxosEvent(io, agent, rs: RetransmitResponse) =>
      handleRetransmitResponse(io, agent, rs)
  }

  val prepareStateFunction: PaxosFunction[RemoteRef] = {
    // nack a low prepare
    case PaxosEvent(io, agent, p@Prepare(id)) if id.number < agent.data.progress.highestPromised =>
      io.send(PrepareNack(id, agent.nodeUniqueId, agent.data.progress, io.journal.bounds.max, agent.data.leaderHeartbeat))
      agent

    // ack a higher prepare
    case PaxosEvent(io, agent, p@Prepare(id)) if id.number > agent.data.progress.highestPromised =>
      handlePromise(io, agent, p)

    // ack prepare that matches our promise
    case PaxosEvent(io, agent, p@Prepare(id)) if id.number == agent.data.progress.highestPromised =>
      io.send(PrepareAck(id, agent.nodeUniqueId, agent.data.progress, io.journal.bounds.max, agent.data.leaderHeartbeat, io.journal.accepted(id.logIndex)))
      agent
  }

  val acceptStateFunction: PaxosFunction[RemoteRef] = {
    // nack lower accept
    case PaxosEvent(io, agent, a@Accept(id, _)) if id.number < agent.data.progress.highestPromised =>
      io.send(AcceptNack(id, agent.nodeUniqueId, agent.data.progress))
      agent

    // nack higher accept for slot which is committed
    case PaxosEvent(io, agent, a@Accept(id, _)) if id.number > agent.data.progress.highestPromised && id.logIndex <= agent.data.progress.highestCommitted.logIndex =>
      io.send(AcceptNack(id, agent.nodeUniqueId, agent.data.progress))
      agent

    // ack accept as high as promise. if id.number > highestPromised must update highest promised in progress http://stackoverflow.com/q/29880949/329496
    case PaxosEvent(io, agent, a@Accept(id, _)) if agent.data.progress.highestPromised <= id.number =>
      handleHighAccept(io, agent, a)
  }

  val ignoreHeartbeatStateFunction: PaxosFunction[RemoteRef] = {
    // ingore a HeartBeat which has not already been handled
    case PaxosEvent(io, agent, HeartBeat) =>
      agent
  }

  /**
   * If no other logic has caught a timeout then do nothing.
   */
  val ignoreNotTimedOutCheck: PaxosFunction[RemoteRef] = {
    case PaxosEvent(_, agent, CheckTimeout) =>
      agent
  }

  val commonStateFunction: PaxosFunction[RemoteRef] =
    retransmissionStateFunction orElse
      prepareStateFunction orElse
      acceptStateFunction orElse
      ignoreHeartbeatStateFunction orElse
      ignoreNotTimedOutCheck

  val notLeaderFunction: PaxosFunction[RemoteRef] = {
    case PaxosEvent(io, agent, v: CommandValue) =>
      io.send(NotLeader(agent.nodeUniqueId, v.msgId))
      agent
  }

  val followerFunction: PaxosFunction[RemoteRef] = followingFunction orElse notLeaderFunction orElse commonStateFunction

  val takeoverFunction: PaxosFunction[RemoteRef] = {
    case PaxosEvent(io, agent, vote: PrepareResponse) =>
      handlePrepareResponse(io, agent, vote)
  }

  val acceptResponseFunction: PaxosFunction[RemoteRef] = {
    case PaxosEvent(io, agent, vote: AcceptResponse) =>
      handleAcceptResponse(io, agent, vote)
  }

  /**
   * Here on a timeout we deal with either pending prepares or pending accepts putting a priority on prepare handling
   * which backs down easily. Only if we have dealt with all timed out prepares do we handle timed out accepts which
   * is more aggressive as it attempts to go-higher than any other node number.
   */
  val resendFunction: PaxosFunction[RemoteRef] = {
    // if we have timed-out on prepare messages
    case PaxosEvent(io, agent, CheckTimeout) if agent.data.prepareResponses.nonEmpty && io.clock > agent.data.timeout =>
      handleResendPrepares(io, agent, io.clock)

    // if we have timed-out on accept messages
    case PaxosEvent(io, agent, CheckTimeout) if agent.data.acceptResponses.nonEmpty && io.clock >= agent.data.timeout =>
      handleResendAccepts(io, agent, io.clock)
  }

  /**
   * If we see a commit at a higher slot we should backdown and request retransmission.
   * If we see a commit for the same slot but with a higher number from a node with a higher node unique id we should backdown.
   * Other commits are ignored.
   */
  val leaderLikeFunction: PaxosFunction[RemoteRef] = {
    case PaxosEvent(io, agent@PaxosAgent(_, _, HighestCommittedIndexAndEpoch(committedLogIndex, epoch)), c@Commit(i@Identifier(_, number, logIndex), _)) if logIndex > committedLogIndex || (logIndex == committedLogIndex && number > epoch) =>
      handleReturnToFollowerOnHigherCommit(io, agent, c)

    case PaxosEvent(io, agent, c: Commit) =>
      agent
  }

  val recoveringFunction: PaxosFunction[RemoteRef] =
    takeoverFunction orElse
      acceptResponseFunction orElse
      resendFunction orElse
      leaderLikeFunction orElse
      notLeaderFunction orElse
      commonStateFunction

  val recovererFunction: PaxosFunction[RemoteRef] = recoveringFunction orElse notLeaderFunction orElse commonStateFunction

  val leaderStateFunction: PaxosFunction[RemoteRef] = {
    // heartbeats the highest commit message
    case PaxosEvent(io, agent, HeartBeat) =>
      io.send(Commit(agent.data.progress.highestCommitted))
      agent

    // broadcasts a new client value
    case PaxosEvent(io, agent, value: CommandValue) =>
      handleClientCommand(io, agent, value, io.sender)

    // ignore late vote as we would have transitioned on a majority ack
    case PaxosEvent(io, agent, value: PrepareResponse) =>
      agent
  }

  val leaderFunction: PaxosFunction[RemoteRef] =
    leaderStateFunction orElse
      acceptResponseFunction orElse
      resendFunction orElse
      leaderLikeFunction orElse
      commonStateFunction

  def apply(e: PaxosEvent[RemoteRef]): PaxosAgent[RemoteRef] = e.agent.role match {
    case Follower => followerFunction(e)
    case Recoverer => recovererFunction(e)
    case Leader => leaderFunction(e)
  }
}
