package com.github.trex_paxos.util

import java.util.Arrays.{equals => bequals}

import com.github.trex_paxos.library._
import org.scalatest.{Matchers, WordSpecLike}

import scala.util.{Failure, Success, Try}

class PickleTests extends WordSpecLike with Matchers {

  "Pickling rich objects" should {

    "roundrip Commit" in {
      val c = Commit(Identifier(1, BallotNumber(2, 3), 4L))
      Pickle.unpack(Pickle.pack(c).iterator) match {
        case `c` =>
        case f => fail(f.toString)
      }
    }
    val bytes1 = Array[Byte](5, 6)
    val bytes2 = Array[Byte](7, 8)

    "roundrip NotLeader" in {
      val n = NotLeader(1, 2.toString)
      Pickle.unpack(Pickle.pack(n).iterator) should be(NotLeader(1, 2.toString))
    }

    "roundrip Prepare" in {
      val p = Prepare(Identifier(1, BallotNumber(2, 3), 4L))
      Pickle.unpack(Pickle.pack(p).iterator) match {
        case `p` =>
        case f => fail(f.toString)
      }
    }

    "roundtrip Accept" in {
      {
        val a = Accept(Identifier(1, BallotNumber(2, 3), 4L), ClientCommandValue("0", bytes1))
        val b = Pickle.pack(a).iterator
        Pickle.unpack(b) match {
          case Accept(Identifier(1, BallotNumber(2, 3), 4L), ClientCommandValue("0", bout)) =>
            assert(bequals(Array[Byte](5, 6), bout))
          case f => fail(f.toString)
        }
      }
      {
        val a = Accept(Identifier(1, BallotNumber(2, 3), 4L), NoOperationCommandValue)
        Pickle.unpack(Pickle.pack(a).iterator) match {
          case `a` =>
          case f => fail(f.toString)
        }
      }
    }
    "roundtrip AcceptAck" in {
      val r = AcceptAck(Identifier(1, BallotNumber(2, 3), 4L), 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)))
      Pickle.unpack(Pickle.pack(r).iterator) match {
        case `r` =>
        case f => fail(f.toString)
      }
    }
    "roundtrip AcceptNack" in {
      val r = AcceptNack(Identifier(1, BallotNumber(2, 3), 4L), 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)))
      Pickle.unpack(Pickle.pack(r).iterator) match {
        case `r` =>
        case f => fail(f.toString)
      }
    }

    "roundtrip PrepareAck" in {
      {
        val p = PrepareAck(Identifier(1, BallotNumber(2, 3), 4L), 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), 12, 13, None)
        Pickle.unpack(Pickle.pack(p).iterator) match {
          case `p` =>
          case f => fail(f.toString)
        }
      }
      {
        val a = Accept(Identifier(1, BallotNumber(2, 3), 4L), ClientCommandValue("0", bytes1))
        val p = PrepareAck(Identifier(1, BallotNumber(2, 3), 4L), 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), 12, 13, Option(a))
        Pickle.unpack(Pickle.pack(p).iterator) match {
          case PrepareAck(Identifier(1, BallotNumber(2, 3), 4L), 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), 12, 13, Some(a)) =>
            a match {
              case Accept(Identifier(1, BallotNumber(2, 3), 4L), ClientCommandValue("0", bout)) =>
                assert(bequals(Array[Byte](5, 6), bout))
              case f => fail(f.toString)
            }
          case f => fail(f.toString)
        }
      }
    }
    "roundtrip PrepareNack" in {
      val p = PrepareNack(Identifier(1, BallotNumber(2, 3), 4L), 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), 12, 13)
      Pickle.unpack(Pickle.pack(p).iterator) match {
        case `p` =>
        case f => fail(f.toString)
      }
    }
    "roundtrip RetransmitRequest" in {
      val r = RetransmitRequest(1, 2, 3L)
      Pickle.unpack(Pickle.pack(r).iterator) match {
        case `r` =>
        case f => fail(f.toString)
      }
    }
    "roundtrip simple RetransmitResponse" in {
      val a1 = Accept(Identifier(1, BallotNumber(2, 3), 4L), ClientCommandValue("0", bytes1))
      val a2 = Accept(Identifier(5, BallotNumber(6, 7), 8L), ClientCommandValue("0", bytes2))
      val r = RetransmitResponse(10, 11, Seq(a1), Seq(a2))
      val b = Pickle.pack(r).iterator
      Pickle.unpack(b) match {
        case RetransmitResponse(10, 11, Seq(a1), Seq(a2)) => {
          a1 match {
            case Accept(Identifier(1, BallotNumber(2, 3), 4L), ClientCommandValue("0", bout)) =>
              assert(bequals(Array[Byte](5, 6), bout))
            case f => fail(f.toString)
          }
          a2 match {
            case Accept(Identifier(5, BallotNumber(6, 7), 8L), ClientCommandValue("0", bout)) =>
              assert(bequals(Array[Byte](7, 8), bout))
            case f => fail(f.toString)
          }
        }
        case f => fail(f.toString)
      }
    }

    def assertAccept(a1: Accept, a2: Accept): Boolean = {
      if (a1.from == a2.from) {
        if (a1.id == a2.id) {
          a1.value match {
            case ClientCommandValue(a1i, b1) =>
              a2.value match {
                case ClientCommandValue(a2i, b2) =>
                  a1i should be(a2i)
                  bequals(b1, b2)
                case f => fail(f.toString)
              }
            case f => fail(f.toString)
          }
        } else false
      } else false

    }

    "roundtrip empty RetransmitResponse" in {
      val a = Accept(Identifier(1, BallotNumber(2, 3), 4L), ClientCommandValue("0", bytes1))

      {
        val r = RetransmitResponse(10, 11, Seq.empty[Accept], Seq.empty[Accept])
        val b = Pickle.pack(r).iterator
        Pickle.unpack(b) match {
          case RetransmitResponse(10, 11, s1, s2) if s1.isEmpty && s2.isEmpty =>
          case f => fail(f.toString)
        }
      }
      {
        val r = RetransmitResponse(10, 11, Seq(a), Seq.empty[Accept])
        val b = Pickle.pack(r).iterator
        Pickle.unpack(b) match {
          case RetransmitResponse(10, 11, s1, s2) if s1.size == 1 && s2.isEmpty =>
            assertAccept(s1.head, a)
          case f => fail(f.toString)
        }
      }
      {
        val r = RetransmitResponse(10, 11, Seq.empty[Accept], Seq(a))
        val b = Pickle.pack(r).iterator
        Pickle.unpack(b) match {
          case RetransmitResponse(10, 11, s1, s2) if s1.isEmpty && s2.size == 1 =>
            assertAccept(s2.head, a)
          case f => fail(f.toString)
        }
      }
    }
    "roundtrip multiple values" in {
      val a1 = Accept(Identifier(1, BallotNumber(1, 1), 1L), ClientCommandValue("0", Array[Byte](1, 1)))
      val a2 = Accept(Identifier(2, BallotNumber(2, 2), 2L), ClientCommandValue("0", Array[Byte](2, 2)))
      val a3 = Accept(Identifier(3, BallotNumber(3, 3), 3L), ClientCommandValue("0", Array[Byte](3, 3)))
      val a4 = Accept(Identifier(4, BallotNumber(4, 4), 4L), ClientCommandValue("0", Array[Byte](4, 4)))
      val r = RetransmitResponse(10, 11, Seq(a1, a2), Seq(a3, a4))
      val b = Pickle.pack(r).iterator
      Pickle.unpack(b) match {
        case RetransmitResponse(10, 11, s1, s2) if s1.size == 2 && s2.size == 2 =>
          assertAccept(s1(0), a1)
          assertAccept(s1(1), a2)
          assertAccept(s2(0), a3)
          assertAccept(s2(1), a4)
        case f => fail(f.toString)
      }
    }
    "roundtrip None ServerResponse" in {
      val s1 = ServerResponse(0, "one", None)
      Pickle.unpack(Pickle.pack(s1).iterator) match {
        case `s1` => // good
        case f => fail(f.toString)
      }
    }
    "roundtrip empty ServerResponse" in {
      val s1 = ServerResponse(0, "one", Some(Array[Byte]()))
      Pickle.unpack(Pickle.pack(s1).iterator) match {
        case ServerResponse(n, m, Some(a)) if n == 0L && m == "one" && a.length == 0 => // good
        case f => fail(f.toString)
      }
    }
    "roundtrip some ServerResponse" in {
      val s1 = ServerResponse(0, "one", Some(bytes1))
      Pickle.unpack(Pickle.pack(s1).iterator) match {
        case ServerResponse(n, m, Some(bout)) if n == 0L && m == "one" =>
          assert(bequals(Array[Byte](5, 6), bout))
        case f => fail(f.toString)
      }
    }

  }
  "Pickling with crc32" should {
    "should not fail CRC if not modified data" in {
      val identifier11: Identifier = Identifier(1, BallotNumber(1, 1), 11L)
      val v1: CommandValue = ClientCommandValue("hello", Array[Byte](1.toByte))
      val a11 = Accept(identifier11, v1)

      val buffer = Pickle.pack(a11).prependCrcData()

      val bytes: Array[Byte] = buffer.toArray

      val modified = ByteChain(bytes).checkCrcData()

      val unpacked = Pickle.unpack(modified.iterator)

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
  "should fail on CRC if modified data" in {
    // if we corrput the first 4 bytes we may think that the size is upto Int.Max so will blow up with out a CRC check failure
    (4 until 19) foreach { index =>
      val result = Try {
        val identifier11: Identifier = Identifier(1, BallotNumber(1, 1), 11L)
        val v1: CommandValue = ClientCommandValue("hello", Array[Byte](1.toByte))
        val a11 = Accept(identifier11, v1)

        val buffer = Pickle.pack(a11).prependCrcData()

        val bytes: Array[Byte] = buffer.toArray

        bytes(index) = (bytes(index) + 1).toByte

        val modified = ByteChain(bytes).checkCrcData()

        Pickle.unpack(modified.iterator)
      }
      result match {
        case Success(f) => fail(f.toString)
        case Failure(ex) => ex match {
          case f: IllegalArgumentException if f.getMessage.contains("CRC32") => // good
          case f => throw f
        }
      }
    }
  }
}