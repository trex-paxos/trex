package com.github.trex_paxos.internals

import com.github.trex_paxos.library._
import org.scalatest.{Matchers, WordSpecLike}

import java.util.Arrays.{equals => bequals}

class PickleTests extends WordSpecLike with Matchers {
  import Pickle._

  "Pickling simple objects " should {
    "roundtrip int" in {
      unpickleInt(pickleInt(Int.MaxValue)) should be(Int.MaxValue)
      unpickleInt(pickleInt(Int.MinValue)) should be(Int.MinValue)
      unpickleInt(pickleInt(0)) should be(0)
      unpickleInt(pickleInt(1)) should be(1)
      unpickleInt(pickleInt(99)) should be(99)
      unpickleInt(pickleInt(-99)) should be(-99)
      unpickleInt(pickleInt(0xff)) should be(0xff)
      unpickleInt(pickleInt(0xffff)) should be(0xffff)
      unpickleInt(pickleInt(-1 * 0xffff)) should be(-1 * 0xffff)
      unpickleInt(pickleInt(0xfffe)) should be(0xfffe)
      unpickleInt(pickleInt(-1 * 0xfffe)) should be(-1 * 0xfffe)
      unpickleInt(pickleInt(0xffffff)) should be(0xffffff)
      unpickleInt(pickleInt(-1 * 0xffffff)) should be(-1 * 0xffffff)
      unpickleInt(pickleInt(0xfffffe)) should be(0xfffffe)
      unpickleInt(pickleInt(-1 * 0xfffffe)) should be(-1 * 0xfffffe)
      unpickleInt(pickleInt(0xffffffe)) should be(0xffffffe)
      unpickleInt(pickleInt(-1 * 0xffffffe)) should be(-1 * 0xffffffe)

      0 until 32 foreach { shift =>
        val i = 1 << shift
        unpickleInt(pickleInt(i)) should be(i)
        val minusI = -1 * i
        unpickleInt(pickleInt(minusI)) should be(minusI)
      }
    }

    "roundtrip long" in {
      0 until 64 foreach { shift =>
        val i = 1.toLong << shift
        unpickleLong(pickleLong(i)) should be(i)
        val minusI = -1 * i
        unpickleLong(pickleLong(minusI)) should be(minusI)
      }

      unpickleLong(pickleLong(0.toLong)) should be(0)
      unpickleLong(pickleLong(99.toLong)) should be(99)
      unpickleLong(pickleLong(-99.toLong)) should be(-99)
      unpickleLong(pickleLong(Long.MaxValue)) should be(Long.MaxValue)
      unpickleLong(pickleLong(Long.MinValue)) should be(Long.MinValue)
    }
  }

  "Pickling rich objects" should {

    "roundrip Commit" in {
      val c = Commit(Identifier(1, BallotNumber(2, 3), 4L))
      Pickle.unpack(Pickle.pack(c)) match {
        case `c` =>
      }
    }
    val bytes1 = Array[Byte](5, 6)
    val bytes2 = Array[Byte](7, 8)

    "roundrip NotLeader" in {
      val n = NotLeader(1, 2)
      Pickle.unpack(Pickle.pack(n)) should be(NotLeader(1, 2))
    }

    "roundrip Prepare" in {
      val p = Prepare(Identifier(1, BallotNumber(2, 3), 4L))
      Pickle.unpack(Pickle.pack(p)) match {
        case `p` =>
      }
    }

    "roundtrip Accept" in {
      {
        val a = Accept(Identifier(1, BallotNumber(2, 3), 4L), ClientRequestCommandValue(0, bytes1))
        val b = Pickle.pack(a)
        Pickle.unpack(b) match {
          case Accept(Identifier(1, BallotNumber(2, 3), 4L), ClientRequestCommandValue(0, bout)) =>
            assert(bequals(Array[Byte](5, 6), bout))
        }
      }
      {
        val a = Accept(Identifier(1, BallotNumber(2, 3), 4L), NoOperationCommandValue)
        Pickle.unpack(Pickle.pack(a)) match {
          case `a` =>
        }
      }
      {
        val a = Accept(Identifier(1, BallotNumber(2, 3), 4L), MembershipCommandValue(99L, Seq(ClusterMember(1, "x", true), ClusterMember(2, "y", false))))
        Pickle.unpack(Pickle.pack(a)) match {
          case `a` =>
        }
      }
    }
    "roundtrip AcceptAck" in {
      val r = AcceptAck(Identifier(1, BallotNumber(2, 3), 4L), 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)))
      Pickle.unpack(Pickle.pack(r)) match {
        case `r` =>
      }
    }
    "roundtrip AcceptNack" in {
      val r = AcceptNack(Identifier(1, BallotNumber(2, 3), 4L), 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)))
      Pickle.unpack(Pickle.pack(r)) match {
        case `r` =>
      }
    }

    "roundtrip PrepareAck" in {
      {
        val p = PrepareAck(Identifier(1, BallotNumber(2, 3), 4L), 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), 12, 13, None)
        Pickle.unpack(Pickle.pack(p)) match {
          case `p` =>
          case f => fail(f.toString)
        }
      }
      {
        val a = Accept(Identifier(1, BallotNumber(2, 3), 4L), ClientRequestCommandValue(0, bytes1))
        val p = PrepareAck(Identifier(1, BallotNumber(2, 3), 4L), 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), 12, 13, Option(a))
        Pickle.unpack(Pickle.pack(p)) match {
          case PrepareAck(Identifier(1, BallotNumber(2, 3), 4L), 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), 12, 13, Some(a)) =>
            a match {
              case Accept(Identifier(1, BallotNumber(2, 3), 4L), ClientRequestCommandValue(0, bout)) =>
                assert(bequals(Array[Byte](5, 6), bout))
              case f => fail(f.toString)
            }
        }
      }
    }
    "roundtrip PrepareNack" in {
      val p = PrepareNack(Identifier(1, BallotNumber(2, 3), 4L), 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), 12, 13)
      Pickle.unpack(Pickle.pack(p)) match {
        case `p` =>
        case f => fail(f.toString)
      }
    }
    "roundtrip RetransmitRequest" in {
      val r = RetransmitRequest(1, 2, 3L)
      Pickle.unpack(Pickle.pack(r)) match {
        case `r` =>
        case f => fail(f.toString)
      }
    }
    "roundtrip simple RetransmitResponse" in {
      val a1 = Accept(Identifier(1, BallotNumber(2, 3), 4L), ClientRequestCommandValue(0, bytes1))
      val a2 = Accept(Identifier(5, BallotNumber(6, 7), 8L), ClientRequestCommandValue(0, bytes2))
      val r = RetransmitResponse(10, 11, Seq(a1), Seq(a2))
      val b = Pickle.pack(r)
      Pickle.unpack(b) match {
        case RetransmitResponse(10, 11, Seq(a1), Seq(a2)) => {
          a1 match {
            case Accept(Identifier(1, BallotNumber(2, 3), 4L), ClientRequestCommandValue(0, bout)) =>
              assert(bequals(Array[Byte](5, 6), bout))
            case f => fail(f.toString)
          }
          a2 match {
            case Accept(Identifier(5, BallotNumber(6, 7), 8L), ClientRequestCommandValue(0, bout)) =>
              assert(bequals(Array[Byte](7, 8), bout))
            case f => fail(f.toString)
          }
        }
      }
    }

    def assertAccept(a1: Accept, a2: Accept): Boolean = {
      if (a1.from == a2.from) {
        if (a1.id == a2.id) {
          a1.value match {
            case ClientRequestCommandValue(a1i, b1) =>
              a2.value match {
                case ClientRequestCommandValue(a2i, b2) =>
                  a1i should be(a2i)
                  bequals(b1, b2)
                case f => fail(f.toString)
              }
          }
        } else false
      } else false

    }

    "roundtrip empty RetransmitResponse" in {
      val a = Accept(Identifier(1, BallotNumber(2, 3), 4L), ClientRequestCommandValue(0, bytes1))

      {
        val r = RetransmitResponse(10, 11, Seq.empty[Accept], Seq.empty[Accept])
        val b = Pickle.pack(r)
        Pickle.unpack(b) match {
          case RetransmitResponse(10, 11, s1, s2) if s1.isEmpty && s2.isEmpty =>
          case f => fail(f.toString)
        }
      }
      {
        val r = RetransmitResponse(10, 11, Seq(a), Seq.empty[Accept])
        val b = Pickle.pack(r)
        Pickle.unpack(b) match {
          case RetransmitResponse(10, 11, s1, s2) if s1.size == 1 && s2.isEmpty =>
            assertAccept(s1.head, a)
          case f => fail(f.toString)
        }
      }
      {
        val r = RetransmitResponse(10, 11, Seq.empty[Accept], Seq(a))
        val b = Pickle.pack(r)
        Pickle.unpack(b) match {
          case RetransmitResponse(10, 11, s1, s2) if s1.isEmpty && s2.size == 1 =>
            assertAccept(s2.head, a)
          case f => fail(f.toString)
        }
      }
    }
    "roundtrip multiple values" in {
      val a1 = Accept(Identifier(1, BallotNumber(1, 1), 1L), ClientRequestCommandValue(0, Array[Byte](1, 1)))
      val a2 = Accept(Identifier(2, BallotNumber(2, 2), 2L), ClientRequestCommandValue(0, Array[Byte](2, 2)))
      val a3 = Accept(Identifier(3, BallotNumber(3, 3), 3L), ClientRequestCommandValue(0, Array[Byte](3, 3)))
      val a4 = Accept(Identifier(4, BallotNumber(4, 4), 4L), ClientRequestCommandValue(0, Array[Byte](4, 4)))
      val r = RetransmitResponse(10, 11, Seq(a1, a2), Seq(a3, a4))
      val b = Pickle.pack(r)
      Pickle.unpack(b) match {
        case RetransmitResponse(10, 11, s1, s2) if s1.size == 2 && s2.size == 2 =>
          assertAccept(s1(0), a1)
          assertAccept(s1(1), a2)
          assertAccept(s2(0), a3)
          assertAccept(s2(1), a4)
        case f => fail(f.toString)
      }
    }
  }
}
