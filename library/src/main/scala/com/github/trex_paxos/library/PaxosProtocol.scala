package com.github.trex_paxos.library

import scala.compat.Platform

/**
 * We perform consensus over instances of CommandValue.
 */
trait CommandValue extends PaxosMessage {
  val emptyArray: Array[Byte] = Array.empty[Byte]

  def bytes: Array[Byte]

  def msgUuid: String
}

/**
  * Client request command has an id to correlate to the server response.
  * @param msgUuid The client message id used to correlate sent commands back to server responses.
  * @param bytes The serialized client command read to log to a journal or transit on the wire.
  */
case class ClientCommandValue(msgUuid: String, val bytes: Array[Byte]) extends CommandValue

/**
  * Client request command has an id to correlate to the server response. Does not need to be made durable and can be lost during crashes.
  * @param msgUuid The client message id used to correlate sent commands back to server responses.
  * @param bytes The serialized client command read to log to a journal or transit on the wire.
  */
case class ReadOnlyClientCommandValue(msgUuid: String, val bytes: Array[Byte]) extends CommandValue

/**
  * Cluster administration command which has an id to correlate to the server response.
  * @param msgUuid The message id used to correlate sent commands back to server responses.
  * @param bytes The serialized client command read to log to a journal or transit on the wire.
  */
case class ClusterCommandValue(msgUuid: String, val bytes: Array[Byte]) extends CommandValue


case object NoOperationCommandValue extends CommandValue {
  def bytes = emptyArray

  val msgUuid = ""
}


/**
  * Once consensus has been reached we deliver CommandValues in consensus order with the possibility of repeats during crash recovery.
  * @param logIndex The Paxos slow under which the command value is committed. My be used to deduplicate repeated deliveries due to crashes.
  * @param command The value which has been chosen by the consensus protocol.
  */
case class Payload(logIndex: Long, command: CommandValue)

/**
 * The logical number used to discriminate messages as either higher or lower. Numbers must be unique to _both_ the node in the cluster *and* paxos prepare.  Physically it is 64bits with high 32bits an epoch number and low 32bits a node unique identifier. The number will be fixed for a stable leader so it also represents a leaders term.
 * @param counter Used by candidate leaders to "go higher" than prior or competing leaders. No guarantees are made as to this number; there may be gaps between values issued by a node and there may be collisions between dueling leaders.
 * @param nodeIdentifier node unique number which must be unique to an agent within the cluster (e.g. set from unique configuration or parsed from DNS name ’node0’, ’node1'). This value is used to tie break between dueling leaders. Safety of the algorithm requires that this value must be unique per cluster.
 */
case class BallotNumber(counter: Int, nodeIdentifier: Int) {
  def >(that: BallotNumber) = if (this == that) false else if (this.counter > that.counter) true else if (this.counter < that.counter) false else this.nodeIdentifier > that.nodeIdentifier

  def >=(that: BallotNumber) = if (this == that) true else if (this.counter > that.counter) true else if (this.counter < that.counter) false else this.nodeIdentifier > that.nodeIdentifier

  def <(that: BallotNumber) = if (this == that) false else if (this.counter > that.counter) false else if (this.counter < that.counter) true else this.nodeIdentifier < that.nodeIdentifier

  def <=(that: BallotNumber) = if (this == that) true else if (this.counter > that.counter) false else if (this.counter < that.counter) true else this.nodeIdentifier < that.nodeIdentifier

  override def toString = f"N(c=${counter.toLong},n=$nodeIdentifier)"
}

/**
 * Identifies a unique leader epoch and log index “slot” into which a value may be proposed. Each leader must only propose a single value into any given slot and must change the [[BallotNumber]] to propose a different value at the same slot. The identifier used for a given slot will be shared across prepare, accept and commit messages during a leader take-over. The ordering of identifiers is defined by their log index order which is used to commit accepted values in order.
 * TODO how can from differ from number.nodeIdentifier?
 * @param from The node sending the message.
 * @param number The paxos proposal number used for comparing messages, or values, as higher or lower than each other. This value will be fixed for a stable leadership.
 * @param logIndex The contiguous log stream position, or “slot”, into which values are proposed and committed in order.
 */
case class Identifier(val from: Int, val number: BallotNumber, val logIndex: Long) {
  override def toString = f"I(f=$from,n=$number,s=$logIndex)"
}

/**
 * The progress of a node is the highest promise number and the highest committed message. 
 * @param highestPromised Highest promise made by a node
 * @param highestCommitted Highest position and highest number committed by a node
 */
case class Progress(val highestPromised: BallotNumber, val highestCommitted: Identifier) {
  override def toString = s"P(p=$highestPromised,c=$highestCommitted)"
}

object Progress {
  val highestPromisedLens = Lens(
    get = (_: Progress).highestPromised,
    set = (progress: Progress, promise: BallotNumber) => progress.copy(highestPromised = promise)
  )

  val highestCommittedLens = Lens(
    get = (_: Progress).highestCommitted,
    set = (progress: Progress, committed: Identifier) => progress.copy(highestCommitted = committed)
  )

  val highestPromisedHighestCommitted: Lens[Progress,(BallotNumber,Identifier)] = Lens(
    get = (p: Progress) => ((highestPromisedLens(p), highestCommittedLens(p))),
    set = (p: Progress, value: (BallotNumber,Identifier)) => value match {
      case
    (promise: BallotNumber, committed: Identifier) =>
    highestPromisedLens.set(highestCommittedLens.set(p, committed), promise)

    }
  )
}

/**
 * Marker trait for messages
 */
sealed trait PaxosMessage
/**
 * Prepare is only sent to either establish a leader else to probe for the uncommitted values of a previous leader during the leader take-over phase. Followers must:
 *
 * 1. Check the [[BallotNumber]] of the [[Identifier]] against the highest value previously acknowledged; if the request is lower acknowledged negatively acknowledge (“nack") it.
 * 1. Check the logIndex of the [[Identifier]] against the highest committed logIndex; if the request is lower nack it.
 * 1. If the [[BallotNumber]] of the [[Identifier]] is higher the then previously acknowledged the node must make the new number durable and promise to nack any messages with a lower [[BallotNumber]]  The positive acknowledgement ("ack") must return the highest uncommitted [[Accept]] message with the same log index or None if there is no uncommitted value at that slot.
 */
case class Prepare(id: Identifier) extends PaxosMessage

/**
 * Base type for a response to a prepare message. It provides additional information beyond that prescribed by the core Paxos algorithm which is used during the leader takeover protocol and to prevent unnecessary leader failover attempts.
 */
trait PrepareResponse extends PaxosMessage {
  /**
   * @return The identifier of the [[Prepare]] message being acknowledged.
   */
  def requestId: Identifier

  /**
   * @return The respondent nodeIdentifier
   */
  def from: Int

  /**
   * @return The high commit mark and high promise mark of the responding node.
   */
  def progress: Progress

  /**
   * @return The highest slot this node has accepted a message. Used by a new leader during the leader takeover protocol to expand the range of slots it is recovering.
   */
  def highestAcceptedIndex: Long

  /**
   * @return The last seen leader heartbeat. Used to detect a working leader behind a partial network partition to prevent unnecessary leader failover attempts. 
   */
  def leaderHeartbeat: Long

  /**
    * @return The recipient nodeIdentifier
    */
  def to: Int
}

/**
 * Positively acknowledge a [[Prepare]] message. See [[PrepareResponse]]
 *
 * @param highestAcceptedIndex The highest uncommitted log index accepted by the responding node.
 */
case class PrepareAck(requestId: Identifier, from: Int, progress: Progress, highestAcceptedIndex: Long, leaderHeartbeat: Long, highestUncommitted: Option[Accept]) extends PrepareResponse {
  override def to: Int = requestId.from
}

/**
 * Negatively acknowledge a [[Prepare]] message. See [[PrepareResponse]]
 */
case class PrepareNack(requestId: Identifier, from: Int, progress: Progress, highestAcceptedIndex: Long, leaderHeartbeat: Long) extends PrepareResponse {
  override def to: Int = requestId.from
}

/**
 * Accept proposes a value into a log index position. Followers must:
 *
 * 1. Nack if a promise has been made to a [[Prepare]] request with a higher [[BallotNumber]].
 * 1. Request retransmission of lost messages if the logIndex leads to a gap in log index sequence.
 * 1. If the Ack with a positive response then journal the accept message at the log index if the number is higher than the message currently store at this index.
 *
 * @param id Unique identifier for this request.
 * @param value The value to accept at the slot position indicated by the id
 */
case class Accept(id: Identifier, value: CommandValue) extends PaxosMessage {
  /**
   * @return The unique identifier of the sender within the cluster.
   */
  def from = id.number.nodeIdentifier
}

/**
 * Base type for a response to an accept message.
 */
trait AcceptResponse extends PaxosMessage {
  /**
   * @return The request being negatively acknowledged
   */
  def requestId: Identifier

  /**
   * @return The respondent nodeIdentifier
   */
  def from: Int

  /**
   * @return The high commit mark and high promise mark of the responding node.
   */
  def progress: Progress

  /**
    * @return The recipient nodeIdentifier
    */
  def to: Int
}

/**
 * Positive acknowledgement that the request has been made durable by the respondent.
 * @param requestId The request being positively acknowledged.
 * @param from The unique identifier of the respondent
 */
case class AcceptAck(requestId: Identifier, from: Int, progress: Progress) extends AcceptResponse {
  override def to: Int = requestId.from
}

/**
 * Negative acknowledgement that the request has been rejected by the respondent. The progress in the reply gives an indication of the reason: either the respondent has made a higher promise else the respondent has committed the proposed slot.
 * @param requestId The request being negatively acknowledged
 * @param from The unique identifier of the respondent
 * @param progress The high commit mark and last promise of the responding node.
 */
case class AcceptNack(requestId: Identifier, from: Int, progress: Progress) extends AcceptResponse {
  override def to: Int = requestId.from
}

/**
 * Commit messages indicate the highest committed log stream number. The leader shall heartbeat this message type to indicate that it is alive. Followers must:
 *
 * 1. Commit the specified message in the log index if-and-only-if all previous values have been committed in order.
 * 1. Request retransmission of any messages not known to have been committed at lower log index slot.
 *
 * Note that the leader must commit messages in log index order itself which implies that any prior slots user the same leader number have also been committed by the leader.
 *
 * @param identifier Identifies the unique accept message, and hence unique value, which is being committed into the identified slot.
 * @param heartbeat A value which changes for each heartbeat message which indicates that the leader is alive. 
 */
case class Commit(identifier: Identifier, heartbeat: Long) extends PaxosMessage {
  override def toString = s"Commit(${identifier},h=${heartbeat})"
}

object Commit {
  def apply(identifier: Identifier) = new Commit(identifier, Platform.currentTime)
}

/**
 * Requests retransmission of accept messages higher than a given log message
 * @param from The node unique id which is sending the request
 * @param to The node unique id to which the request is to be routed
 * @param logIndex The log index last committed by the requester
 */
case class RetransmitRequest(from: Int, to: Int, logIndex: Long) extends PaxosMessage

/**
 * Response to a retransmit request
 * @param from The node unique id which is sending the response
 * @param to The node unique id to which the request is to be routed
 * @param committed A contiguous sequence of committed accept messages in ascending order
 * @param uncommitted A contiguous sequence of proposed but uncommitted accept messages in ascending order
 */
case class RetransmitResponse(from: Int, to: Int, val committed: Seq[Accept], uncommitted: Seq[Accept]) extends PaxosMessage

/**
 * Scheduled message used to trigger timeout work.
 */
case object CheckTimeout extends PaxosMessage

/**
 * Scheduled message use by a leader to heatbeat commit messages.
 */
case object HeartBeat extends PaxosMessage

/**
 * Response to a client when the node is not currently the leader. The client should retry the message to another node
 * in the cluster. Note the leader may have crashed and the responding node may become the leader next.
 * @param nodeId The node replying that it is not the leader.
 * @param msgId The client message identifier which the node is responding to.
 */
case class NotLeader(val nodeId: Int, val msgId: String) extends PaxosMessage

/**
  * Response to a client when the nodes is not currently a follower. As the leader is write bottleneck applications that
  * can tolerate stale can reads can opt to spread some reads across all the replicas. This message is return in response
  * to such follower reads. The client can retry other nodes (or even this node) until it gets a response.
  *
  * @param nodeId The node replying that it is has become the leader.
  * @param msgId The client message which the node is responding to.
  */
case class NotFollower(val nodeId: Int, val msgId: String) extends PaxosMessage

/**
 * Response to a client when the nodes has lost its leadership whilst servicing a request during a fail-over due to
 * either a network partition or a long stall. The outcome of the client operation indicated by msgUuid is unknown as the
 * operation may or may not be committed by the new leader. The application will have to query data to learn whether the
 * operation did actually work. Note that semantically this is no different from sending a tcp request to an open socket
 * and not getting back a response; its not known whether the request was processed as there has been neither positive
 * nor negative acknowledgement. Trex doesn't know if it is safe to retry the operation nor how to query to check it
 * happened so the host application will have to decided what to do next. Note that this may be thrown for read only
 * work if the application used strong or single reads as those go via the leader to avoid returning stale data.
 *
 * @param nodeId The node replying that it is has lost the leader.
 * @param msgId The client message which the node is responding to.
 */
case class LostLeadershipException(val nodeId: Int, val msgId: String) extends RuntimeException with PaxosMessage {
  override def toString() = s"LostLeadershipException($nodeId,$msgId)"
}

/**
  *
  * @param logIndex The paxos slot at which the command was chosen.
  * @param clientMsgId The id of the ClientCommandValue being responded to.
  * @param response The result of running the comand value.
  */
case class ServerResponse(logIndex: Long, clientMsgId: String, val response: Option[Array[Byte]])

/** Paxos process roles */
sealed trait PaxosRole

case object Follower extends PaxosRole

case object Recoverer extends PaxosRole

case object Leader extends PaxosRole
