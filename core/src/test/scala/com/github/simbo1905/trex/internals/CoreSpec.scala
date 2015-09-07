package com.github.simbo1905.trex.internals

import java.util.Arrays.{equals => bequals}

import akka.actor.ActorRef
import org.scalatest.{Matchers, WordSpecLike}

import scala.collection.immutable.{TreeMap, SortedMap}

class CoreSpec extends WordSpecLike with Matchers {
  "Paxos Numbers" should {
    "have working equalities" in {
      assert(BallotNumber(2, 2) > BallotNumber(1, 2))
      assert(BallotNumber(2, 2) > BallotNumber(2, 1))
      assert(!(BallotNumber(2, 2) > BallotNumber(2, 2)))

      assert(BallotNumber(2, 2) >= BallotNumber(1, 2))
      assert(BallotNumber(2, 2) >= BallotNumber(2, 1))
      assert(BallotNumber(2, 2) >= BallotNumber(2, 2))

      assert(BallotNumber(1, 1) < BallotNumber(2, 1))
      assert(BallotNumber(1, 1) < BallotNumber(1, 2))
      assert(!(BallotNumber(1, 2) < BallotNumber(1, 2)))

      assert(BallotNumber(1, 1) <= BallotNumber(2, 1))
      assert(BallotNumber(1, 1) <= BallotNumber(1, 2))
      assert(BallotNumber(1, 2) <= BallotNumber(1, 2))

      assert(!(BallotNumber(2,1) <= BallotNumber(1,1)))
    }
  }

  "Lens" should {

    import Ordering._

    val nodeData = PaxosData(null, 0, 0, 3)
    val id = Identifier(0, BallotNumber(1, 2), 3L)

    "set prepare responses" in {
      {
        val newData = PaxosData.prepareResponsesLens.set(nodeData, SortedMap.empty[Identifier, Map[Int, PrepareResponse]])
        assert(newData == nodeData)
      }
      {
        val prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]] = TreeMap(id -> Map.empty)
        val newData = PaxosData.prepareResponsesLens.set(nodeData, prepareResponses)
        assert(newData.prepareResponses(id) == Map.empty)
      }
    }

    "set accept responses" in {
      {
        val newData = PaxosData.acceptResponsesLens.set(nodeData, SortedMap.empty[Identifier, AcceptResponsesAndTimeout])
        assert(newData == nodeData)
      }
      {
        val a1 = Accept(Identifier(1, BallotNumber(1, 1), 1L), ClientRequestCommandValue(0, Array[Byte](1, 1)))
        val acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout] = TreeMap(id -> AcceptResponsesAndTimeout(0L, a1, Map.empty))
        val newData = PaxosData.acceptResponsesLens.set(nodeData, acceptResponses)
        assert(newData.acceptResponses(id) == AcceptResponsesAndTimeout(0L, a1, Map.empty))
      }
    }

    "set client commands" in {
      {
        val newData = PaxosData.clientCommandsLens.set(nodeData, Map.empty[Identifier, (CommandValue, ActorRef)])
        assert(newData == nodeData)
      }
      {
        val commandValue = new CommandValue {
          override def msgId: Long = 0L

          override def bytes: Array[Byte] = Array()
        }
        val clientCommands = Map(id ->(commandValue, null))
        val newData = PaxosData.clientCommandsLens.set(nodeData, clientCommands)
        assert(newData.clientCommands(id) == (commandValue -> null))
      }
    }

    "set leader state" in {
      {
        val newData = PaxosData.leaderLens.set(nodeData, (
          SortedMap.empty[Identifier, Map[Int, PrepareResponse]],
          SortedMap.empty[Identifier, AcceptResponsesAndTimeout],
          Map.empty[Identifier, (CommandValue, ActorRef)])
        )
        assert(newData == nodeData)
      }
      {
        val prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]] = TreeMap(id -> Map.empty)
        val a1 = Accept(Identifier(1, BallotNumber(1, 1), 1L), ClientRequestCommandValue(0, Array[Byte](1, 1)))
        val acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout] = TreeMap(id -> AcceptResponsesAndTimeout(0L, a1, Map.empty))
        val commandValue = new CommandValue {
          override def msgId: Long = 0L

          override def bytes: Array[Byte] = Array()
        }
        val clientCommands = Map(id ->(commandValue, null))
        val newData = PaxosData.leaderLens.set(nodeData, (prepareResponses, acceptResponses, clientCommands))
        assert(newData.prepareResponses(id) == Map.empty)
        assert(newData.acceptResponses(id) == AcceptResponsesAndTimeout(0L, a1, Map.empty))
        assert(newData.clientCommands(id) == (commandValue -> null))
      }
    }
  }

  "Pickling simple objects " should {
    import Pickle._
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
            }
        }
      }
    }
    "roundtrip PrepareNack" in {
      val p = PrepareNack(Identifier(1, BallotNumber(2, 3), 4L), 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), 12, 13)
      Pickle.unpack(Pickle.pack(p)) match {
        case `p` =>
      }
    }
    "roundtrip RetransmitRequest" in {
      val r = RetransmitRequest(1, 2, 3L)
      Pickle.unpack(Pickle.pack(r)) match {
        case `r` =>
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
          }
          a2 match {
            case Accept(Identifier(5, BallotNumber(6, 7), 8L), ClientRequestCommandValue(0, bout)) =>
              assert(bequals(Array[Byte](7, 8), bout))
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
              }
          }
        } else false
      } else false

    }

    "roundrip empty RetransmitResponse" in {
      val a = Accept(Identifier(1, BallotNumber(2, 3), 4L), ClientRequestCommandValue(0, bytes1))

      {
        val r = RetransmitResponse(10, 11, Seq.empty[Accept], Seq.empty[Accept])
        val b = Pickle.pack(r)
        Pickle.unpack(b) match {
          case RetransmitResponse(10, 11, s1, s2) if s1.isEmpty && s2.isEmpty =>
        }
      }
      {
        val r = RetransmitResponse(10, 11, Seq(a), Seq.empty[Accept])
        val b = Pickle.pack(r)
        Pickle.unpack(b) match {
          case RetransmitResponse(10, 11, s1, s2) if s1.size == 1 && s2.isEmpty =>
            assertAccept(s1.head, a)
        }
      }
      {
        val r = RetransmitResponse(10, 11, Seq.empty[Accept], Seq(a))
        val b = Pickle.pack(r)
        Pickle.unpack(b) match {
          case RetransmitResponse(10, 11, s1, s2) if s1.isEmpty && s2.size == 1 =>
            assertAccept(s2.head, a)
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
      }
    }
  }

  "Backing down" should {
    "should reset Paxos data" in {
      import Ordering._
      // given lots of leadership data
      val number = BallotNumber(Int.MinValue, Int.MinValue)
      val id = Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), 0)
      val leaderData = PaxosData(
        progress = Progress(
          number, id
        ),
        leaderHeartbeat = 0,
        timeout = 0,
        clusterSize = 3,
        prepareResponses = TreeMap(id -> Map.empty),
        epoch = Some(number),
        acceptResponses = TreeMap(id -> AcceptResponsesAndTimeout(0, null, Map.empty)),
        clientCommands = Map(id ->(null, null))
      )
      // when we backdown
      val followerData = PaxosActor.backdownData(leaderData, 99L)
      // then it has a new timeout and the leader data is gone
      followerData.timeout shouldBe 99L
      followerData.prepareResponses.isEmpty shouldBe true
      followerData.epoch shouldBe None
      followerData.acceptResponses.isEmpty shouldBe true
      followerData.clientCommands.isEmpty shouldBe true
    }
  }
}