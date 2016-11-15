package com.github.trex_paxos.util

import java.util.NoSuchElementException

import com.github.trex_paxos.library._

import scala.annotation.tailrec

/**
  * An append only and iterable vector of byte arrays. This class does not take defensive copies until you convert
  * it to a Array[Byte] using the toBytes method.
  */
class ByteChain( val bytes: Vector[Array[Byte]], val length: Int) extends Iterable[Byte] {

  override def iterator: Iterator[Byte] = new ByteChain.ByteChainIterator(bytes.map(_.iterator))

  def ++(that: ByteChain): ByteChain = {
    val appended: Vector[Array[Byte]]= this.bytes ++ that.bytes
    val l = this.length + that.length
    new ByteChain(appended, l)
  }

  override def toString(): String = s"ByteChain($length,$bytes)"

  def toBytes: Array[Byte] = {
    val i = this.iterator
    Array.fill[Byte](this.length)(i.next())
  }
}

object ByteChain {

  val empty: ByteChain = ByteChain(Array())

  def apply(bytes: Array[Byte]) = new ByteChain(Vector(bytes), bytes.length)

  class ByteChainIterator(delegatees: Vector[Iterator[Byte]]) extends Iterator[Byte] {

    private var remainder = delegatees

    override def hasNext: Boolean = remainder.headOption match {
      case Some(v) => v.hasNext
      case _  => false
    }

    override def next(): Byte = {
      remainder.headOption match {
        case Some(v) =>
          if( v.hasNext ) {
            val n = v.next()
            if (!v.hasNext) {
              if (remainder.nonEmpty) {
                remainder = remainder.drop(1)
              }
            }
            n
          } else {
            throw new NoSuchElementException
          }
        case _  => throw new NoSuchElementException
      }
    }
  }
}

/**
  * Binary pickling.
  * Note that RetransmitResponse could be big so in the future we might send am identifier handle in the message
  * and stream the actual value over TCP or UTD.
  */
object Pickle {

  val config: Map[Byte, (Class[_], Iterable[Byte] => AnyRef)] = Map(
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
    0xa.toByte -> (classOf[Progress] -> unpickleProgress _),
    0xb.toByte -> (NoOperationCommandValue.getClass -> unpickleNoOpValue _),
    0xc.toByte -> (classOf[ClientCommandValue] -> unpickleClientValue _),
    0xd.toByte -> (classOf[ReadOnlyClientCommandValue] -> unpickleReadOnlyClientValue _),
    0xe.toByte -> (classOf[ClusterCommandValue] -> unpickleClusterValue _),
    0xf.toByte -> (classOf[ServerResponse] -> unpickleServerResponse _)
  )
  val toMap: Map[Class[_], Byte] = (config.map {
    case (b, (c, f)) => c -> b
  }).toMap
  val fromMap: Map[Byte, Iterable[Byte] => AnyRef] = config.map {
    case (b, (c, f)) => b -> f
  }
  val lengthOfInt = 4
  val lengthOfLong = 8
  val lengthOfBool = 1
  val sizeOfBallotNumber = 2 * lengthOfInt
  val sizeOfIdentifier = lengthOfInt + sizeOfBallotNumber + lengthOfLong
  val sizeOfProgress = sizeOfBallotNumber + sizeOfIdentifier

  @inline def unsigned(b: Byte): Int = if (b >= 0) {
    b
  } else {
    256 + b
  }

  def pickleInt(i: Int): ByteChain =
    ByteChain(Array[Byte](
      (i >>> 24).toByte,
      (i >>> 16).toByte,
      (i >>> 8).toByte,
      i.toByte))

  def unpickleInt(b: Iterable[Byte]): Int =
    (unsigned(b.take(1).last) << 24) +
      (unsigned(b.drop(1).take(1).last) << 16) +
      (unsigned(b.drop(2).take(1).last) << 8) +
      unsigned(b.drop(3).take(1).last)

  def pickleLong(i: Long): ByteChain = ByteChain(Array[Byte](
    (i >>> 56).toByte,
    (i >>> 48).toByte,
    (i >>> 40).toByte,
    (i >>> 32).toByte,
    (i >>> 24).toByte,
    (i >>> 16).toByte,
    (i >>> 8).toByte,
    i.toByte)
  )

  def unpickleLong(b: Iterable[Byte]): Long = {
    val (high, low) = b.splitAt(4)
    (unpickleInt(high).toLong << 32) + (unpickleInt(low) & 0xFFFFFFFFL)
  }

  def pickleStringUtf8(s: String): ByteChain = {
    val bytes = s.getBytes("UTF8")
    pickleInt(bytes.length) ++ ByteChain(bytes)
  }

  def unpickleStringUtf8(b: Iterable[Byte]): String = {
    val (l, r) = b.splitAt(lengthOfInt)
    val length = unpickleInt(l)
    new String(r.take(length).toArray, "UTF8")
  }

  def pickleIdentifier(id: Identifier): ByteChain =
    pickleInt(id.from) ++
      pickleInt(id.number.counter) ++
      pickleInt(id.number.nodeIdentifier) ++
      pickleLong(id.logIndex)

  def unpickleIdentifier(b: Iterable[Byte]): Identifier = {
    val id = b.take(sizeOfIdentifier)
    val (from, r1) = id.splitAt(lengthOfInt)
    val (counter, r2) = r1.splitAt(lengthOfInt)
    val (nodeIdentifier, logIndex) = r2.splitAt(lengthOfInt)
    Identifier(unpickleInt(from), BallotNumber(unpickleInt(counter), unpickleInt(nodeIdentifier)), unpickleLong(logIndex))
  }

  def pickleCommit(c: Commit): ByteChain = {
    pickleIdentifier(c.identifier) ++ pickleLong(c.heartbeat)
  }

  def unpickleCommit(b: Iterable[Byte]): Commit = {
    Commit(unpickleIdentifier(b), unpickleLong(b.drop(sizeOfIdentifier)))
  }

  def pickleValue(v: CommandValue) = v match {
    case n@NoOperationCommandValue => pickleNoOpValue
    case c: CommandValue => pickleCommandValue(c)
  }

  def pickleNoOpValue = ByteChain.empty

  def unpickleNoOpValue(b: Iterable[Byte]): CommandValue = NoOperationCommandValue

  def pickleCommandValue(c: CommandValue): ByteChain = pickleStringUtf8(c.msgUuid) ++ pickleInt(c.bytes.length) ++ ByteChain(c.bytes)

  def unpickleClientValue(b: Iterable[Byte]): ClientCommandValue = {
    val (id: String, r2: Iterable[Byte], lv: Int) = unpackValue(b)
    ClientCommandValue(id, r2.take(lv).toArray)
  }

  def unpickleReadOnlyClientValue(b: Iterable[Byte]): ReadOnlyClientCommandValue = {
    val (id: String, r2: Iterable[Byte], lv: Int) = unpackValue(b)
    ReadOnlyClientCommandValue(id, r2.take(lv).toArray)
  }

  def unpickleClusterValue(b: Iterable[Byte]): ClusterCommandValue = {
    val (id: String, r2: Iterable[Byte], lv: Int) = unpackValue(b)
    ClusterCommandValue(id, r2.take(lv).toArray)
  }

  def unpackValue(b: Iterable[Byte]): (String, Iterable[Byte], Int) = {
    val (l0, r0) = b.splitAt(lengthOfInt)
    val lid = unpickleInt(l0)
    val (i, r1) = r0.splitAt(lid)
    val id = new String(i.take(lid).toArray)
    val (l1, r2) = r1.splitAt(lengthOfInt)
    val lv = unpickleInt(l1)
    (id, r2, lv)
  }

  def pickleNotLeader(n: NotLeader): ByteChain = pickleInt(n.nodeId) ++ pickleStringUtf8(n.msgId)

  def unpickleNotLeader(b: Iterable[Byte]): NotLeader = {
    val (nodeId, r0) = b.splitAt(lengthOfInt)
    val (l0, r1) = r0.splitAt(lengthOfInt)
    val lid = unpickleInt(l0)
    val msgId = new String(r1.take(lid).toArray, "UTF8")
    NotLeader(unpickleInt(nodeId), msgId)
  }

  def pickleServerResponse(r: ServerResponse): ByteChain = pickleLong(r.logIndex) ++
    pickleStringUtf8(r.clientMsgId) ++ (r.response match {
    case None =>
      ByteChain(Array(0.toByte))
    case Some(b) =>
      ByteChain(Array(1.toByte)) ++ pickleInt(b.length) ++ ByteChain(b)
    })

  val zeroByte = 0x0.toByte

  def unpickleServerResponse(b: Iterable[Byte]): ServerResponse = {
    val (slot, r0 ) = b.splitAt(lengthOfLong)
    val logIndex = unpickleLong(slot)
    val (l, r1) = r0.splitAt(lengthOfInt)
    val lid = unpickleInt(l)
    val (s, r2) = r1.splitAt(lid)
    val msgId = new String(s.take(lid).toArray, "UTF8")
    val (optArray, r3) = r2.splitAt(1)
    val bb: Byte = optArray.iterator.next()
    val opt = bb match {
      case b if b == zeroByte =>
        None
      case _ =>
        val (l, r4) = r3.splitAt(lengthOfInt)
        val length = unpickleInt(l)
        Some(r4.take(length).toArray)
    }
    ServerResponse(logIndex, msgId, opt)
  }

  def picklePrepare(p: Prepare): ByteChain = pickleIdentifier(p.id)

  def unpicklePrepare(b: Iterable[Byte]): Prepare = Prepare(unpickleIdentifier(b))

  // we have a descrimnator which indicates the type of the command value
  def pickleAccept(a: Accept): ByteChain = pickleIdentifier(a.id) ++ ByteChain(Array(toMap(a.value.getClass))) ++ pickleValue(a.value)

  def unpickleAccept(b: Iterable[Byte]): Accept = {
    val (id, value) = b.splitAt(sizeOfIdentifier)
    Accept(unpickleIdentifier(id), fromMap(value.take(1).last)(value.drop(1)).asInstanceOf[CommandValue])
  }

  def pickleProgress(p: Progress): ByteChain = pickleInt(p.highestPromised.counter) ++
    pickleInt(p.highestPromised.nodeIdentifier) ++ pickleIdentifier(p.highestCommitted)

  def unpickleProgress(b: Iterable[Byte]): Progress = {
    val (counter, r1) = b.splitAt(lengthOfInt)
    val (nodeIdentifier, highestCommitted) = r1.splitAt(lengthOfInt)
    Progress(BallotNumber(unpickleInt(counter), unpickleInt(nodeIdentifier)), unpickleIdentifier(highestCommitted))
  }

  def pickleAcceptAck(a: AcceptAck): ByteChain = pickleIdentifier(a.requestId) ++
    pickleInt(a.from) ++ pickleProgress(a.progress)

  def unpickleAcceptAck(b: Iterable[Byte]): AcceptAck = {
    val (requestId, r1) = b.splitAt(sizeOfIdentifier)
    val (from, progress) = r1.splitAt(lengthOfInt)
    AcceptAck(unpickleIdentifier(requestId), unpickleInt(from), unpickleProgress(progress))
  }

  def pickleAcceptNack(a: AcceptNack): ByteChain = pickleIdentifier(a.requestId) ++ pickleInt(a.from) ++
    pickleProgress(a.progress)

  def unpickleAcceptNack(b: Iterable[Byte]): AcceptNack = {
    val (requestId, r1) = b.splitAt(sizeOfIdentifier)
    val (from, progress) = r1.splitAt(lengthOfInt)
    AcceptNack(unpickleIdentifier(requestId), unpickleInt(from), unpickleProgress(progress))
  }

  def picklePrepareNack(p: PrepareNack): ByteChain = pickleIdentifier(p.requestId) ++ pickleInt(p.from) ++ pickleProgress(p.progress) ++ pickleLong(p.highestAcceptedIndex) ++ pickleLong(p.leaderHeartbeat)

  def unpicklePrepareNack(b: Iterable[Byte]): PrepareNack = {
    val (requestId: Iterable[Byte], from: Iterable[Byte], progress: Iterable[Byte], r3: Iterable[Byte]) = extractPrepare(b)
    val (highestAcceptedIndex, leaderHeartbeat) = r3.splitAt(lengthOfLong)
    PrepareNack(unpickleIdentifier(requestId), unpickleInt(from), unpickleProgress(progress), unpickleLong(highestAcceptedIndex), unpickleLong(leaderHeartbeat))
  }

  def extractPrepare(b: Iterable[Byte]): (Iterable[Byte], Iterable[Byte], Iterable[Byte], Iterable[Byte]) = {
    val (requestId, r1) = b.splitAt(sizeOfIdentifier)
    val (from, r2) = r1.splitAt(lengthOfInt)
    val (progress, r3) = r2.splitAt(sizeOfProgress)
    (requestId, from, progress, r3)
  }

  def picklePrepareAck(p: PrepareAck): ByteChain = {
    val optionOfAccept = if (p.highestUncommitted.isDefined) ByteChain(Array(1.toByte)) ++ pickleAccept(p.highestUncommitted.get) else ByteChain(Array(0.toByte))
    pickleIdentifier(p.requestId) ++ pickleInt(p.from) ++ pickleProgress(p.progress) ++ pickleLong(p.highestAcceptedIndex) ++ pickleLong(p.leaderHeartbeat) ++ optionOfAccept
  }

  def unpicklePrepareAck(b: Iterable[Byte]): PrepareAck = {
    val (requestId: Iterable[Byte], from: Iterable[Byte], progress: Iterable[Byte], r3: Iterable[Byte]) = extractPrepare(b)

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

  def pickleRetransmitRequest(r: RetransmitRequest): ByteChain = pickleInt(r.from) ++ pickleInt(r.to) ++ pickleLong(r.logIndex)

  def unpickleRetransitRequest(b: Iterable[Byte]): RetransmitRequest = {
    val (from, r1) = b.splitAt(lengthOfInt)
    val (to, logIndex) = r1.splitAt(lengthOfInt)
    RetransmitRequest(unpickleInt(from), unpickleInt(to), unpickleLong(logIndex))
  }

  def pickleSeqAccept(seq: Seq[Accept]): ByteChain = {
    seq.foldLeft(pickleInt(seq.length)) { (b, a) =>
      val ap = pickleAccept(a)
      val l = ap.length
      // here we pickle the length so that we know how many bytes to drop to scroll to the next accept
      b ++ pickleInt(l) ++ ap
    }
  }

  def pickleRetransmitResponse(r: RetransmitResponse): ByteChain = {
    val b = pickleInt(r.from) ++
      pickleInt(r.to) ++
      pickleSeqAccept(r.committed) ++
      pickleSeqAccept(r.uncommitted)
    b
  }

  def unpickleSeqAccept(bytes: Iterable[Byte]): (Seq[Accept], Iterable[Byte]) = {
    @tailrec
    def ups(count: Int, b: Iterable[Byte], seq: Seq[Accept]): (Seq[Accept], Iterable[Byte]) = {
      if (count == 0) {
        (seq, b)
      } else {
        val (length, abytes) = b.splitAt(lengthOfInt)
        val l = unpickleInt(length)
        ups(count - 1, abytes.drop(l), seq :+ unpickleAccept(abytes))
      }
    }
    val (count, remainder) = bytes.splitAt(lengthOfInt)
    ups(unpickleInt(count), remainder, Seq.empty)
  }

  def unpickleRetransmitResponse(b: Iterable[Byte]): RetransmitResponse = {

    val (from, fromRemainder) = b.splitAt(lengthOfInt)
    val (to, toRemainder) = fromRemainder.splitAt(lengthOfInt)

    val (committed, committedRemainder) = unpickleSeqAccept(toRemainder)
    val (uncommitted, uncommittedRemainder) = unpickleSeqAccept(committedRemainder)
    require(uncommittedRemainder.isEmpty)
    RetransmitResponse(unpickleInt(from), unpickleInt(to), committed, uncommitted)
  }

  def pickle(a: AnyRef): ByteChain = a match {
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
    case NoOperationCommandValue => pickleNoOpValue
    case s: ServerResponse => pickleServerResponse(s)
    case x =>
      System.err.println(s"don't know how to pickle $x so returning empty ByteChain")
      ByteChain.empty
  }

  def unpack(b: Array[Byte]): AnyRef = fromMap(b.head)(b.tail)

  def pack(a: AnyRef): ByteChain = ByteChain(Array(toMap(a.getClass))) ++ pickle(a)
}
