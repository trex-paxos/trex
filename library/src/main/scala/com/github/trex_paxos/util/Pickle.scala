package com.github.trex_paxos.util

import java.util.NoSuchElementException
import java.util.zip.CRC32

import com.github.trex_paxos.library._

import scala.annotation.tailrec

/**
  * An append only and iterable vector of byte arrays. This class does not take defensive copies until you convert
  * it to a Array[Byte] using the toBytes method.
  */
class ByteChain(val bytes: Vector[Array[Byte]], val length: Int) extends Iterable[Byte] {

  override def iterator: Iterator[Byte] = new ByteChain.ByteChainIterator(bytes.map(_.iterator))

  def ++(that: ByteChain): ByteChain = {
    val appended: Vector[Array[Byte]] = this.bytes ++ that.bytes
    val l = this.length + that.length
    new ByteChain(appended, l)
  }

  override def toString(): String = s"ByteChain($length,$bytes)"

  def toBytes: Array[Byte] = {
    val i = this.iterator
    Array.fill[Byte](this.length)(i.next())
  }

  import PicklePositiveIntegers._

  def pickleSignedInt(i: Int): ByteChain =
    ByteChain(Array[Byte](
      (i >>> 24).toByte,
      (i >>> 16).toByte,
      (i >>> 8).toByte,
      i.toByte))

  def unpickleSignedInt(b: Iterator[Byte]): Int =
    (unsigned(b.next()) << 24) +
      (unsigned(b.next()) << 16) +
      (unsigned(b.next()) << 8) +
      unsigned(b.next())


  /**
    * @return returns a new ByteChain with the length and CRC32 of this one prepended to message integrity checks
    */
  def prependCrcData(): ByteChain = {
    val crc = new CRC32()
    bytes.foreach(crc.update(_))
    val crc32 = crc.getValue.toInt // yep the first 4 bytes of the long are always blank it really is a 32bit crc
    pickleInt(this.length) ++ pickleSignedInt(crc32) ++ this
  }

  /**
    * Will throw java.lang.IllegalArgumentException if the crc32 of the remainder does not match the crc32 in bytes 4 thru 7
    * @return drops the first 8 bytes, interprets the first 4 as the length of the remainder and the second as its crc32
    *
    */
  def checkCrcData(): ByteChain = {
    val i = this.iterator
    val len = unpickleInt(i)
    val expectedCrc32 = unpickleSignedInt(i)
    val payload: Array[Byte] = Array.fill[Byte](len)(i.next())
    val crc = new CRC32()
    crc.update(payload)
    val actualCrc32 = crc.getValue.toInt
    if (actualCrc32 != expectedCrc32) {
      throw new IllegalArgumentException(s"CRC32 check ${actualCrc32} != ${expectedCrc32}")
    }
    ByteChain(payload)
  }
}

object ByteChain {

  val empty: ByteChain = ByteChain(Array())

  def apply(bytes: Array[Byte]) = new ByteChain(Vector(bytes), bytes.length)

  class ByteChainIterator(delegatees: Vector[Iterator[Byte]]) extends Iterator[Byte] {

    private var remainder = delegatees

    override def hasNext: Boolean = remainder.headOption match {
      case Some(v) => v.hasNext
      case _ => false
    }

    override def next(): Byte = {
      remainder.headOption match {
        case Some(v) =>
          if (v.hasNext) {
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
        case _ => throw new NoSuchElementException
      }
    }
  }
}

/**
  * Pickles positive integers in a 7 bit big-endian format. The leading byte has a bit header of zero to eight consecutive
  * 1s that indicates how many following bytes are part of the number. A leading 0 bit says no following bytes implying
  * 7 bits of data whereas a leading 1110 says three following bytes implying 28 bits of data.
  */
object PicklePositiveIntegers {

  /**
    * @param maxBits Bits of data
    * @param header Leading bit header of zero or more consecutive 1s.
    * @param extractHeader Mask to extract the header.
    * @param extractBody Mask to remove the header.
    */
  case class Constants(maxBits: Int, header: Int, extractHeader: Int, extractBody: Int) {
    val bytes = maxBits / 7
  }

  @inline def unsigned(b: Byte): Int = if (b >= 0) {
    b
  } else {
    256 + b
  }

  val c0 = Constants(7, 0x0, 0x80, 0x7f)
  val c1 = Constants(14, 0x80, 0xC0, 0x3f)
  val c2 = Constants(21, 0xC0, 0xE0, 0x1F)
  val c3 = Constants(28, 0xE0, 0xF0, 0x0F)
  val c4 = Constants(35, 0xF0, 0xF8, 0x07)
  val c5 = Constants(42, 0xF8, 0xFC, 0x03)
  val c6 = Constants(49, 0xFC, 0xFE, 0x01)
  val c7 = Constants(56, 0xFE, 0xFF, 0x0)
  val c8 = Constants(63, 0xFF, 0xFF, 0x0)

  val constants = Seq(c0, c1, c2, c3, c4, c5, c6, c7, c8)

  val numberMaxSizes: Array[Long] = (constants map {
    c => Math.pow(2, c.maxBits).toLong
  }).toArray

  def constantsFor(l: Long) = {
    require(l >= 0)

    def lookupIndex(l: Long): Int = {
      numberMaxSizes.foldLeft(0) { (index, long) =>
        if (l < long) return index
        else index + 1
      }
    }

    constants(Math.min(lookupIndex(l), 8))
  }

  def pickleInt(i: Int): ByteChain = pickleLong(i)

  def unpickleInt(i: Iterator[Byte]): Int = unpickleLong(i).toInt

  def pickleLong(i: Long): ByteChain = {

    val constant = constantsFor(i)

    def pack(bytes: List[Byte], bs: Int): List[Byte] = {
      if (bytes.length >= constant.bytes) bytes
      else pack((i >>> bs).toByte :: bytes, bs + 8)
    }

    val bytes = pack(List(), 0).toArray

    bytes(0) = (bytes(0) | constant.header).toByte

    ByteChain(bytes)
  }

  def unpickleLong(i: Iterator[Byte]): Long = {

    val firstWithHeader: Byte = unsigned(i.next()).toByte

    val constant = (constants dropWhile {
      c => (firstWithHeader & c.extractHeader) != c.header
    }).head

    val firstWithoutHeader: Byte = (firstWithHeader & constant.extractBody).toByte

    def takeBytes(count: Int, bs: List[Byte]): List[Byte] = {
      if (count - 1 > 0) takeBytes(count - 1, i.next() :: bs)
      else bs
    }

    val bytes = takeBytes( constant.bytes, List(firstWithoutHeader))

    // above 57 bits we end up with 9 bytes the first of which is leading zeros we need to drop
    val longBytes = if (bytes.length <= 8) bytes else bytes.take(8)

    val value: Long = longBytes.zipWithIndex.foldLeft(0L) {
      case (cumulative: Long, (b: Byte, index: Int)) =>
        cumulative + (unsigned(b).toLong << (8 * index))
    }

    value
  }

}

class ByteIterator(bs: Array[Byte]) extends Iterator[Byte]{
  var index = 0
  override def hasNext: Boolean = index < bs.length

  override def next(): Byte = {
    val b = bs(index)
    index = index + 1
    b
  }
}


/**
  * Binary pickling.
  * Note that RetransmitResponse could be big so in the future we might send am identifier handle in the message
  * and stream the actual value over TCP or UTD.
  */
object Pickle {

  val UTF8 = "UTF8"

  def jbytesToIteratorByte(bs: Array[Byte] ): Iterator[Byte] ={
    new ByteIterator(bs);
  }

  val config: Map[Byte, (Class[_], Iterator[Byte] => AnyRef)] = Map(
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

  val fromMap: Map[Byte, Iterator[Byte] => AnyRef] = config.map {
    case (b, (c, f)) => b -> f
  }

  import PicklePositiveIntegers._

  def pickleStringUtf8(s: String): ByteChain = {
    val bytes = s.getBytes(UTF8)
    pickleInt(bytes.length) ++ ByteChain(bytes)
  }

  def unpickleStringUtf8(b: Iterator[Byte]): String = {
    val length = unpickleInt(b)
    new String(b.take(length).toArray, UTF8)
  }

  def pickleIdentifier(id: Identifier): ByteChain =
    pickleInt(id.from) ++
      pickleInt(id.number.counter) ++
      pickleInt(id.number.nodeIdentifier) ++
      pickleLong(id.logIndex)

  def unpickleIdentifier(i: Iterator[Byte]): Identifier = {
    Identifier(unpickleInt(i), BallotNumber(unpickleInt(i), unpickleInt(i)), unpickleLong(i))
  }

  def pickleCommit(c: Commit): ByteChain = {
    pickleIdentifier(c.identifier) ++ pickleLong(c.heartbeat)
  }

  def unpickleCommit(b: Iterator[Byte]): Commit = {
    Commit(unpickleIdentifier(b), unpickleLong(b))
  }

  def pickleValue(v: CommandValue) = v match {
    case n@NoOperationCommandValue => pickleNoOpValue
    case c: CommandValue => pickleCommandValue(c)
  }

  def pickleNoOpValue = ByteChain.empty

  def unpickleNoOpValue(b: Iterator[Byte]): CommandValue = NoOperationCommandValue

  def pickleCommandValue(c: CommandValue): ByteChain = pickleStringUtf8(c.msgUuid) ++ pickleInt(c.bytes.length) ++ ByteChain(c.bytes)

  def unpickleClientValue(b: Iterator[Byte]): ClientCommandValue = {
    val (id: String, lv: Int) = unpackMsgIdAndLength(b)
    ClientCommandValue(id, b.take(lv).toArray)
  }

  def unpickleReadOnlyClientValue(b: Iterator[Byte]): ReadOnlyClientCommandValue = {
    val (id: String, lv: Int) = unpackMsgIdAndLength(b)
    ReadOnlyClientCommandValue(id, b.take(lv).toArray)
  }

  def unpickleClusterValue(b: Iterator[Byte]): ClusterCommandValue = {
    val (id: String, lv: Int) = unpackMsgIdAndLength(b)
    ClusterCommandValue(id, b.take(lv).toArray)
  }

  def unpackMsgIdAndLength(b: Iterator[Byte]): (String, Int) = {
    val lid = unpickleInt(b)
    val id = new String(b.take(lid).toArray)
    val lv = unpickleInt(b)
    (id, lv)
  }

  def pickleNotLeader(n: NotLeader): ByteChain = pickleInt(n.nodeId) ++ pickleStringUtf8(n.msgId)

  def unpickleNotLeader(i: Iterator[Byte]): NotLeader = {
    val nodeId = unpickleInt(i)
    val lid = unpickleInt(i)
    val msgId = new String(i.take(lid).toArray, UTF8)
    NotLeader(nodeId, msgId)
  }

  def pickleServerResponse(r: ServerResponse): ByteChain = pickleLong(r.logIndex) ++
    pickleStringUtf8(r.clientMsgId) ++ (r.response match {
    case None =>
      ByteChain(Array(0.toByte))
    case Some(b) =>
      ByteChain(Array(1.toByte)) ++ pickleInt(b.length) ++ ByteChain(b)
  })

  val zeroByte = 0x0.toByte

  def unpickleServerResponse(b: Iterator[Byte]): ServerResponse = {
    val logIndex = unpickleLong(b)
    val lid = unpickleInt(b)
    val msgId = new String(b.take(lid).toArray, UTF8)
    val bb: Byte = b.next()
    val opt = bb match {
      case b if b == zeroByte =>
        None
      case _ =>
        val length = unpickleInt(b)
        Some(b.take(length).toArray)
    }
    ServerResponse(logIndex, msgId, opt)
  }

  def picklePrepare(p: Prepare): ByteChain = pickleIdentifier(p.id)

  def unpicklePrepare(b: Iterator[Byte]): Prepare = Prepare(unpickleIdentifier(b))

  // this uses a discriminator to indicate the type of the command value
  def pickleAccept(a: Accept): ByteChain = pickleIdentifier(a.id) ++ ByteChain(Array(toMap(a.value.getClass))) ++ pickleValue(a.value)

  def unpickleAccept(b: Iterator[Byte]): Accept = {
    Accept(unpickleIdentifier(b), fromMap(b.next())(b).asInstanceOf[CommandValue])
  }

  def pickleProgress(p: Progress): ByteChain = pickleInt(p.highestPromised.counter) ++
    pickleInt(p.highestPromised.nodeIdentifier) ++ pickleIdentifier(p.highestCommitted)

  def unpickleProgress(b: Iterator[Byte]): Progress = {
    Progress(BallotNumber(unpickleInt(b), unpickleInt(b)), unpickleIdentifier(b))
  }

  def pickleAcceptAck(a: AcceptAck): ByteChain = pickleIdentifier(a.requestId) ++
    pickleInt(a.from) ++ pickleProgress(a.progress)

  def unpickleAcceptAck(b: Iterator[Byte]): AcceptAck = {
    AcceptAck(unpickleIdentifier(b), unpickleInt(b), unpickleProgress(b))
  }

  def pickleAcceptNack(a: AcceptNack): ByteChain = pickleIdentifier(a.requestId) ++ pickleInt(a.from) ++
    pickleProgress(a.progress)

  def unpickleAcceptNack(b: Iterator[Byte]): AcceptNack = {
    AcceptNack(unpickleIdentifier(b), unpickleInt(b), unpickleProgress(b))
  }

  def picklePrepareNack(p: PrepareNack): ByteChain = pickleIdentifier(p.requestId) ++ pickleInt(p.from) ++ pickleProgress(p.progress) ++ pickleLong(p.highestAcceptedIndex) ++ pickleLong(p.leaderHeartbeat)

  def unpicklePrepareNack(b: Iterator[Byte]): PrepareNack = {
    PrepareNack(unpickleIdentifier(b), unpickleInt(b), unpickleProgress(b), unpickleLong(b), unpickleLong(b))
  }

  def picklePrepareAck(p: PrepareAck): ByteChain = {
    val optionOfAccept =
      if (p.highestUncommitted.isDefined)
        ByteChain(Array(1.toByte)) ++ pickleAccept(p.highestUncommitted.get)
      else
        ByteChain(Array(0.toByte))
    optionOfAccept ++ pickleIdentifier(p.requestId) ++ pickleInt(p.from) ++ pickleProgress(p.progress) ++ pickleLong(p.highestAcceptedIndex) ++ pickleLong(p.leaderHeartbeat)
  }

  def unpicklePrepareAck(b: Iterator[Byte]): PrepareAck = {
    val optAccept = b.next() match {
      case 0x0 => None
      case _ => Some(unpickleAccept(b))
    }
    PrepareAck(unpickleIdentifier(b), unpickleInt(b), unpickleProgress(b), unpickleLong(b), unpickleLong(b), optAccept)
  }

  def pickleRetransmitRequest(r: RetransmitRequest): ByteChain = pickleInt(r.from) ++ pickleInt(r.to) ++ pickleLong(r.logIndex)

  def unpickleRetransitRequest(b: Iterator[Byte]): RetransmitRequest = {
    RetransmitRequest(unpickleInt(b), unpickleInt(b), unpickleLong(b))
  }

  def pickleSeqAccept(seq: Seq[Accept]): ByteChain = {
    seq.foldLeft(pickleInt(seq.length)) { (b, a) =>
      b ++ pickleAccept(a)
    }
  }

  def unpickleSeqAccept(i: Iterator[Byte]): Seq[Accept] = {
    val count = unpickleInt(i)
    0.until(count).map(_ => unpickleAccept(i))
  }

  def pickleRetransmitResponse(r: RetransmitResponse): ByteChain = {
    pickleInt(r.from) ++
      pickleInt(r.to) ++
      pickleSeqAccept(r.committed) ++
      pickleSeqAccept(r.uncommitted)
  }

  def unpickleRetransmitResponse(b: Iterator[Byte]): RetransmitResponse = {
    RetransmitResponse(unpickleInt(b), unpickleInt(b), unpickleSeqAccept(b), unpickleSeqAccept(b))
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

  def unpack(b: Iterator[Byte]): AnyRef = fromMap(b.next())(b)

  def pack(a: AnyRef): ByteChain = ByteChain(Array(toMap(a.getClass))) ++ pickle(a)
}
