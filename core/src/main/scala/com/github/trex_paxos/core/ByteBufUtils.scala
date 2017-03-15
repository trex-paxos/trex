package com.github.trex_paxos.core

import com.github.trex_paxos.util.{ByteChain, Pickle}
import io.netty.buffer.{ByteBuf, Unpooled}

object ByteBufUtils {
  import scala.language.implicitConversions
  implicit def byteChainToByteBuf(chain: ByteChain): ByteBuf = {
    val buffer: ByteBuf = Unpooled.buffer(chain.length)
    chain.bytes.foldLeft( 0 ) {
      ( offset, bytes) =>
        buffer.setBytes(offset, bytes)
        offset + bytes.length
    }

    buffer
  }
  implicit def byteBufToIterableByte(buf: ByteBuf): Iterable[Byte] = {
    new ByteBufIterable(buf)
  }
}

class ByteBufIterable(byteBuf: ByteBuf) extends Iterable[Byte] {

  class ByteBufIterator(byteBuf: ByteBuf) extends Iterator[Byte] {
    val capacity = byteBuf.capacity()
    var index = 0

    override def hasNext: Boolean = index < capacity

    override def next(): Byte = {
      val b = byteBuf.getByte(index)
      index = index + 1
      b
    }
  }

  override def iterator: Iterator[Byte] = new ByteBufIterator(byteBuf)
}
