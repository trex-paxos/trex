package com.github.trex_paxos.netty

import com.github.trex_paxos.util.ByteChain
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

object ByteBufIterable {
  def apply(byteBuf: ByteBuf) = new ByteBufIterable(byteBuf)
}

class ByteBufIterable(byteBuf: ByteBuf) extends Iterable[Byte] {

  class ByteBufIterator(byteBuf: ByteBuf) extends Iterator[Byte] {
    override def hasNext: Boolean = byteBuf.isReadable()

    override def next(): Byte = byteBuf.readByte()
  }

  override def iterator: Iterator[Byte] = new ByteBufIterator(byteBuf)
}
