package com.github.trex_paxos.core

import com.github.trex_paxos.util.{ByteChain, Pickle}

object DriverPickle {

  val UTF8 = "UTF8"

  val config: Map[Byte, (Class[_], Iterator[Byte] => AnyRef)] = Map(
    0xf0.toByte -> (classOf[ServerResponse] -> unpickleServerResponse _)
  )
  val toMap: Map[Class[_], Byte] = (config.map {
    case (b, (c, f)) => c -> b
  }).toMap

  val fromMap: Map[Byte, Iterator[Byte] => AnyRef] = config.map {
    case (b, (c, f)) => b -> f
  }

  import com.github.trex_paxos.util.PicklePositiveIntegers._

  val zeroByte = 0x0.toByte

  def pickleServerResponse(r: ServerResponse): ByteChain = pickleLong(r.logIndex) ++
    Pickle.pickleStringUtf8(r.clientMsgId) ++ (r.response match {
    case None =>
      ByteChain(Array(0.toByte))
    case Some(b) =>
      ByteChain(Array(1.toByte)) ++ pickleInt(b.length) ++ ByteChain(b)
  })

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
  def pickle(a: AnyRef): ByteChain = a match {
    case s: ServerResponse => pickleServerResponse(s)
    case x =>
      System.err.println(s"don't know how to pickle $x so returning empty ByteChain")
      ByteChain.empty
  }

  def unpack(b: Iterator[Byte]): AnyRef = fromMap(b.next)(b)

  def pack(a: AnyRef): ByteChain = ByteChain(Array(toMap(a.getClass))) ++ pickle(a)
}
