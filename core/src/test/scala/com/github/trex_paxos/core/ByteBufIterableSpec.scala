package com.github.trex_paxos.core

import com.github.trex_paxos.library._
import com.github.trex_paxos.util.{ByteChain, Pickle}
import io.netty.buffer.ByteBuf
import org.scalatest.{Matchers, WordSpecLike}

class ByteBufIterableSpec extends WordSpecLike with Matchers {

  "ByteBufIterable" should {
    "should round trip an object" in {
      import ByteBufUtils._

      val identifier11: Identifier = Identifier(1, BallotNumber(1, 1), 11L)
      val v1: CommandValue = ClientCommandValue("hello", Array[Byte](1.toByte))
      val a11 = Accept(identifier11, v1)

      val buffer: ByteBuf = Pickle.pack(a11).prependCrcData()

      val checked: ByteChain = ByteChain(buffer.array())

      val unpacked = Pickle.unpack(checked.checkCrcData().iterator)

      unpacked match {
        case a11@Accept(id, value) =>
          id shouldBe identifier11
          value match {
            case v1@ClientCommandValue(msgUuid, array) =>
              msgUuid shouldBe "hello"
              array(0) shouldBe 1.toByte
          }
        case f => fail(f.toString)
      }
    }
  }
}