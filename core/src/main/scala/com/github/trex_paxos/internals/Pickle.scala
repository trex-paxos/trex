package com.github.trex_paxos.internals

import akka.util.ByteString
import com.github.trex_paxos.library._

import scala.annotation.tailrec

/**
 * Binary pickling. I did try scala pickling but had all sorts of runtime problems uncluding jvm crashes.
 * Note that RetransmitResponse could be big so in the future we might send a handle and
 * have the responder connect back and stream it one accept over time over TCP. That would
 * fit in better with snapshotting.
 */
object Pickle {
  @inline def unsigned(b: Byte): Int = if (b >= 0) {
    b
  } else {
    256 + b
  }

  def pickleInt(i: Int): ByteString = ByteString(Array[Byte](
    (i >>> 24).toByte,
    (i >>> 16).toByte,
    (i >>> 8).toByte,
    i.toByte))

  def unpickleInt(b: ByteString): Int =
    (unsigned(b.take(1).last) << 24) +
      (unsigned(b.drop(1).take(1).last) << 16) +
      (unsigned(b.drop(2).take(1).last) << 8) +
      unsigned(b.drop(3).take(1).last)

  def pickleLong(i: Long): ByteString = ByteString(Array[Byte](
    (i >>> 56).toByte,
    (i >>> 48).toByte,
    (i >>> 40).toByte,
    (i >>> 32).toByte,
    (i >>> 24).toByte,
    (i >>> 16).toByte,
    (i >>> 8).toByte,
    i.toByte)
  )

  def unpickleLong(b: ByteString): Long = {
    val (high, low) = b.splitAt(4)
    (unpickleInt(high).toLong << 32) + (unpickleInt(low) & 0xFFFFFFFFL)
  }

  val config: Map[Byte, (Class[_], ByteString => AnyRef)] = Map(
    0x0.toByte -> (classOf[Accept] -> unpickleAccept _),
    0x1.toByte -> (classOf[AcceptAck] -> unpickleAcceptAck _),
    0x2.toByte -> (classOf[AcceptNack] -> unpickleAcceptNack _),
    0x3.toByte -> (classOf[Commit] -> unpickleCommit _),
    0x4.toByte -> (classOf[NotLeader] -> unpickleNotLeader _),
    0x5.toByte -> (classOf[Prepare] -> unpicklePrepare _),
    0x6.toByte -> (classOf[PrepareNack] -> unpicklePrepareNack _),
    0x7.toByte -> (classOf[PrepareAck] -> unpicklePrepareAck _),
    0x8.toByte -> (classOf[RetransmitRequest] -> unpickleRetransitRequest _),
    0x9.toByte -> (classOf[RetransmitResponse] -> unpickleRetransmitResponse _),
    0xa.toByte -> (classOf[ClientRequestCommandValue] -> unpickleClientValue _),
    0xb.toByte -> (NoOperationCommandValue.getClass -> unpickleNoOpValue _),
    0xc.toByte -> (classOf[MembershipCommandValue] -> unpickleMembershipValue _),
    0xd.toByte -> (classOf[Progress] -> unpickleProgress _),
    0xe.toByte -> (classOf[Membership] -> unpickleMembership _)
  )

  val toMap: Map[Class[_], Byte] = (config.map {
    case (b, (c, f)) => c -> b
  }).toMap

  val fromMap: Map[Byte, ByteString => AnyRef] = config.map {
    case (b, (c, f)) => b -> f
  }

  def pickleIdentifier(id: Identifier): ByteString =
    pickleInt(id.from) ++
      pickleInt(id.number.counter) ++
      pickleInt(id.number.nodeIdentifier) ++
      pickleLong(id.logIndex)

  val lengthOfInt = 4
  val lengthOfLong = 8
  val lengthOfBool = 1
  val sizeOfBallotNumber = 2 * lengthOfInt
  val sizeOfIdentifier = lengthOfInt + sizeOfBallotNumber + lengthOfLong
  val sizeOfProgress = sizeOfBallotNumber + sizeOfIdentifier

  def unpickleIdentifier(b: ByteString): Identifier = {
    val id = b.take(sizeOfIdentifier)
    val (from, r1) = id.splitAt(lengthOfInt)
    val (counter, r2) = r1.splitAt(lengthOfInt)
    val (nodeIdentifier, logIndex) = r2.splitAt(lengthOfInt)
    Identifier(unpickleInt(from), BallotNumber(unpickleInt(counter), unpickleInt(nodeIdentifier)), unpickleLong(logIndex))
  }

  def pickleCommit(c: Commit): ByteString = {
    pickleIdentifier(c.identifier) ++ pickleLong(c.heartbeat)
  }

  def unpickleCommit(b: ByteString): Commit = {
    Commit(unpickleIdentifier(b), unpickleLong(b.drop(sizeOfIdentifier)))
  }

  def pickleValue(v: CommandValue) = v match {
    case n@NoOperationCommandValue => pickleNoOpValue
    case c: ClientRequestCommandValue => pickleClientValue(c)
    case m: MembershipCommandValue => pickleMembershipValue(m)
  }

  def pickleNoOpValue = ByteString()

  def unpickleNoOpValue(b: ByteString): CommandValue = NoOperationCommandValue

  def pickleClientValue(c: ClientRequestCommandValue): ByteString = pickleLong(c.msgId) ++ pickleInt(c.bytes.length) ++ ByteString(c.bytes)

  def unpickleClientValue(b: ByteString): ClientRequestCommandValue = {
    val (i, r1) = b.splitAt(lengthOfLong)
    val (l, remainder) = r1.splitAt(lengthOfInt)
    val length = unpickleInt(l)
    val id = unpickleLong(i)
    ClientRequestCommandValue(id, remainder.take(length).toArray)
  }

  def pickleNotLeader(n: NotLeader): ByteString = pickleInt(n.nodeId) ++ pickleLong(n.msgId)

  def unpickleNotLeader(b: ByteString): NotLeader = {
    val (nodeId, msgId) = b.splitAt(lengthOfInt)
    NotLeader(unpickleInt(nodeId), unpickleLong(msgId))
  }

  def pickleClusterMember(m: ClusterMember) = {
    val bool = if (m.active) ByteString(1.toByte) else ByteString(0.toByte)
    pickleInt(m.nodeUniqueId) ++ pickleInt(m.location.getBytes("UTF8").length) ++ ByteString(m.location.getBytes("UTF8")) ++ bool
  }

  def unpickleClusterMember(bytes: ByteString): (ClusterMember, ByteString) = {
    val (nodeUniqueId, r1) = bytes.splitAt(lengthOfInt)
    val (length, r2) = r1.splitAt(lengthOfInt)
    val l = unpickleInt(length)
    val (location, bool) = r2.splitAt(l)
    val active = bool.take(1).lastOption match {
      case Some(b) if b == 0 => false
      case _ => true
    }
    (ClusterMember(unpickleInt(nodeUniqueId), new String(location.toArray, "UTF8"), active), bool.drop(1))
  }

  def pickleMembershipValue(m: MembershipCommandValue) = pickleLong(m.msgId) ++ pickleInt(m.members.size) ++ m.members.foldLeft(ByteString())((bs, m) => bs ++ pickleClusterMember(m))

  def unpickleMembershipValue(b: ByteString): MembershipCommandValue = {
    val (id, msg) = b.splitAt(lengthOfLong)
    val (size, remainder) = msg.splitAt(lengthOfInt)
    val s = unpickleInt(size)
    def take(b: ByteString, members: Seq[ClusterMember], count: Int): Seq[ClusterMember] = {
      count match {
        case 0 =>
          members
        case _ =>
          val (member, remainder) = unpickleClusterMember(b)
          take(remainder, members :+ member, count - 1)
      }
    }
    val members = take(remainder, Seq.empty, s)
    MembershipCommandValue(unpickleLong(id), members)
  }

  def pickleMembership(m: Membership) =
    pickleInt(m.members.size) ++
      m.members.foldLeft(ByteString())((bs, m) => bs ++ pickleClusterMember(m)) // TODO consolidate with pickleMembershipValue

  def unpickleMembership(b: ByteString): Membership = {
    // TODO consolidate with unpickleMembershipValue
    val (size, remainder) = b.splitAt(lengthOfInt)
    val s = unpickleInt(size)
    def take(b: ByteString, members: Seq[ClusterMember], count: Int): Seq[ClusterMember] = {
      count match {
        case 0 =>
          members
        case _ =>
          val (member, remainder) = unpickleClusterMember(b)
          take(remainder, members :+ member, count - 1)
      }
    }
    val members = take(remainder, Seq.empty, s)
    Membership(members)
  }

  def picklePrepare(p: Prepare): ByteString = pickleIdentifier(p.id)

  def unpicklePrepare(b: ByteString): Prepare = Prepare(unpickleIdentifier(b))

  def pickleAccept(a: Accept): ByteString = pickleIdentifier(a.id) ++ ByteString(toMap(a.value.getClass)) ++ pickleValue(a.value)

  def unpickleAccept(b: ByteString): Accept = {
    val (id, value) = b.splitAt(sizeOfIdentifier)
    Accept(unpickleIdentifier(id), fromMap(value.take(1).last)(value.drop(1)).asInstanceOf[CommandValue])
  }

  def pickleProgress(p: Progress): ByteString = pickleInt(p.highestPromised.counter) ++ pickleInt(p.highestPromised.nodeIdentifier) ++ pickleIdentifier(p.highestCommitted)

  def unpickleProgress(b: ByteString): Progress = {
    val (counter, r1) = b.splitAt(lengthOfInt)
    val (nodeIdentifier, highestCommitted) = r1.splitAt(lengthOfInt)
    Progress(BallotNumber(unpickleInt(counter), unpickleInt(nodeIdentifier)), unpickleIdentifier(highestCommitted))
  }

  def pickleAcceptAck(a: AcceptAck): ByteString = pickleIdentifier(a.requestId) ++ pickleInt(a.from) ++ pickleProgress(a.progress)

  def unpickleAcceptAck(b: ByteString): AcceptAck = {
    val (requestId, r1) = b.splitAt(sizeOfIdentifier)
    val (from, progress) = r1.splitAt(lengthOfInt)
    AcceptAck(unpickleIdentifier(requestId), unpickleInt(from), unpickleProgress(progress))
  }

  def pickleAcceptNack(a: AcceptNack): ByteString = pickleIdentifier(a.requestId) ++ pickleInt(a.from) ++ pickleProgress(a.progress)

  def unpickleAcceptNack(b: ByteString): AcceptNack = {
    val (requestId, r1) = b.splitAt(sizeOfIdentifier)
    val (from, progress) = r1.splitAt(lengthOfInt)
    AcceptNack(unpickleIdentifier(requestId), unpickleInt(from), unpickleProgress(progress))
  }

  def picklePrepareNack(p: PrepareNack): ByteString = pickleIdentifier(p.requestId) ++ pickleInt(p.from) ++ pickleProgress(p.progress) ++ pickleLong(p.highestAcceptedIndex) ++ pickleLong(p.leaderHeartbeat)

  def unpicklePrepareNack(b: ByteString): PrepareNack = {
    val (requestId, r1) = b.splitAt(sizeOfIdentifier)
    val (from, r2) = r1.splitAt(lengthOfInt)
    val (progress, r3) = r2.splitAt(sizeOfProgress)
    val (highestAcceptedIndex, leaderHeartbeat) = r3.splitAt(lengthOfLong)
    PrepareNack(unpickleIdentifier(requestId), unpickleInt(from), unpickleProgress(progress), unpickleLong(highestAcceptedIndex), unpickleLong(leaderHeartbeat))
  }

  def picklePrepareAck(p: PrepareAck): ByteString = {
    val optionOfAccept = if (p.highestUncommitted.isDefined) ByteString(1) ++ pickleAccept(p.highestUncommitted.get) else ByteString(0)
    pickleIdentifier(p.requestId) ++ pickleInt(p.from) ++ pickleProgress(p.progress) ++ pickleLong(p.highestAcceptedIndex) ++ pickleLong(p.leaderHeartbeat) ++ optionOfAccept
  }

  def unpicklePrepareAck(b: ByteString): PrepareAck = {
    val (requestId, r1) = b.splitAt(sizeOfIdentifier)
    val (from, r2) = r1.splitAt(lengthOfInt)
    val (progress, r3) = r2.splitAt(sizeOfProgress)
    val (highestAcceptedIndex, r4) = r3.splitAt(lengthOfLong)
    val (leaderHeartbeat, r5) = r4.splitAt(lengthOfLong)
    val (bool, accept) = r5.splitAt(lengthOfBool)

    bool.lastOption match {
      case Some(0x0) =>
        PrepareAck(unpickleIdentifier(requestId), unpickleInt(from), unpickleProgress(progress), unpickleLong(highestAcceptedIndex), unpickleLong(leaderHeartbeat), None)
      case _ =>
        val a = unpickleAccept(accept)
        PrepareAck(unpickleIdentifier(requestId), unpickleInt(from), unpickleProgress(progress), unpickleLong(highestAcceptedIndex), unpickleLong(leaderHeartbeat), Option(a))
    }
  }

  def pickleRetransmitRequest(r: RetransmitRequest): ByteString = pickleInt(r.from) ++ pickleInt(r.to) ++ pickleLong(r.logIndex)

  def unpickleRetransitRequest(b: ByteString): RetransmitRequest = {
    val (from, r1) = b.splitAt(lengthOfInt)
    val (to, logIndex) = r1.splitAt(lengthOfInt)
    RetransmitRequest(unpickleInt(from), unpickleInt(to), unpickleLong(logIndex))
  }

  def pickleRetransmitResponse(r: RetransmitResponse): ByteString = {
    def pickleSeq(seq: Seq[Accept]): ByteString = {
      seq.foldLeft(pickleInt(seq.length)) { (b, a) =>
        val ap = pickle(a)
        b ++ pickleInt(ap.length) ++ ap
      }
    }
    ByteString() ++
      pickleInt(r.from) ++
      pickleInt(r.to) ++
      pickleSeq(r.committed) ++
      pickleSeq(r.uncommitted)
  }

  def unpickleRetransmitResponse(b: ByteString): RetransmitResponse = {

    val (from, fromRemainder) = b.splitAt(lengthOfInt)
    val (to, toRemainder) = fromRemainder.splitAt(lengthOfInt)

    def unpickleSeq(b: ByteString): (Seq[Accept], ByteString) = {
      @tailrec
      def ups(count: Int, b: ByteString, seq: Seq[Accept]): (Seq[Accept], ByteString) = {
        if (count == 0) {
          (seq, b)
        } else {
          val (length, remainder) = b.splitAt(lengthOfInt)
          val (accept, rest) = remainder.splitAt(unpickleInt(length))
          ups(count - 1, rest, seq :+ unpickleAccept(accept))
        }
      }
      val (count, remainder) = b.splitAt(lengthOfInt)
      ups(unpickleInt(count), remainder, Seq.empty)
    }
    val (committed, committedRemainder) = unpickleSeq(toRemainder)
    val (uncommitted, uncommittedRemainder) = unpickleSeq(committedRemainder)
    require(uncommittedRemainder.isEmpty)
    RetransmitResponse(unpickleInt(from), unpickleInt(to), committed, uncommitted)
  }

  def pickle(a: AnyRef): ByteString = a match {
    case a: Accept => pickleAccept(a)
    case a: AcceptAck => pickleAcceptAck(a)
    case a: AcceptNack => pickleAcceptNack(a)
    case c: Commit => pickleCommit(c)
    case n: NotLeader => pickleNotLeader(n)
    case p: Prepare => picklePrepare(p)
    case p: PrepareNack => picklePrepareNack(p)
    case p: PrepareAck => picklePrepareAck(p)
    case r: RetransmitRequest => pickleRetransmitRequest(r)
    case r: RetransmitResponse => pickleRetransmitResponse(r)
    case p: Progress => pickleProgress(p)
    case m: MembershipCommandValue => pickleMembershipValue(m)
    case m: Membership => pickleMembership(m)
    case x =>
      System.err.println(x)
      ByteString()
  }

  def unpack(b: ByteString): AnyRef = fromMap(b.head)(b.tail)

  def pack(a: AnyRef): ByteString = ByteString(toMap(a.getClass)) ++ pickle(a)
}
