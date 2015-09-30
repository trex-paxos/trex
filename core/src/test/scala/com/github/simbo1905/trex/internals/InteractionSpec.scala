package com.github.simbo1905.trex.internals

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import com.github.simbo1905.trex.library._
import com.github.simbo1905.trex.internals.PaxosActor._
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, SpecLike}

import scala.collection.immutable.{TreeMap, SortedMap}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.postfixOps

object InteractionSpec {
  val config = ConfigFactory.parseString("trex.leader-timeout-min=1\ntrex.leader-timeout-max=10\nakka.loglevel = \"DEBUG\"")
}

class InteractionSpec extends TestKit(ActorSystem("InteractionSpec",
  InteractionSpec.config)) with SpecLike with ImplicitSender with BeforeAndAfterAll with Matchers {

  import Ordering._

  class TestJournal extends Journal {
    var _progress = Journal.minBookwork.copy()
    var _map: SortedMap[Long, Accept] = TreeMap.empty

    def save(progress: Progress): Unit = _progress = progress

    def load(): Progress = _progress

    def accept(accepted: Accept*): Unit = accepted foreach { a =>
      _map = _map + (a.id.logIndex -> a)
    }

    def accepted(logIndex: Long): Option[Accept] = _map.get(logIndex)

    def bounds: JournalBounds = {
      val keys = _map.keys
      if (keys.isEmpty) JournalBounds(0L, 0L) else JournalBounds(keys.head, keys.last)
    }
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val minPrepare = Prepare(Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), Long.MinValue))

  object `A three node cluster` {

    def `should work when two nodes are up` {
      expectNoMsg(25 millisecond)
      // given node zero
      val node0 = new TestJournal
      val actor0 = TestActorRef(new TestPaxosActor(Configuration(InteractionSpec.config, 3), 0, self, node0, ArrayBuffer.empty, None))
      // and node one
      val node1 = new TestJournal
      val actor1 = TestActorRef(new TestPaxosActor(Configuration(InteractionSpec.config, 3), 1, self, node1, ArrayBuffer.empty, None))
      // when node zero times-out
      actor0 ! CheckTimeout
      // it issues a low prepare
      expectMsg(50 millisecond, minPrepare)
      // and node one will nack the load prepare
      actor1 ! minPrepare
      val nack: PrepareNack = expectMsgPF(50 millisecond) { case p: PrepareNack => p }
      // which will cause node zero to issue a higher prepare
      nack.requestId.from should be(0)
      // when we send it back to node zero
      actor0 ! nack
      // it issues a higher prepare
      val phigh: Prepare = expectMsgPF(50 milliseconds) { case hprepare: Prepare => hprepare }
      phigh.id.logIndex should be(1)
      phigh.id.number.nodeIdentifier should be(0)
      // when we send that high prepare to node one
      actor1 ! phigh
      // it should ack
      val pack = expectMsgPF(50 millisecond) { case p: PrepareAck => p }
      pack.requestId should be(phigh.id)
      // when we send that back to node zero
      actor0 ! pack

      // it will issue a noop accept
      val accept: Accept = expectMsgPF(50 millisecond) { case a: Accept => a }

      accept.id.logIndex should be(1)
      accept.value shouldBe NoOperationCommandValue
      accept.id.number should be(phigh.id.number)
      // and ack its own accept
      actor0.underlyingActor.data.acceptResponses match {
        case map if map.nonEmpty =>
          map.get(accept.id) match {
            case None => fail()
            case Some(AcceptResponsesAndTimeout(_, _, responses)) =>
              responses.values.headOption match {
                case Some(a: AcceptAck) => //good
                case x => fail(x.toString)
              }
          }
        case x => fail(x.toString)
      }
      // when we send that to node one
      actor1 ! accept

      // it will ack
      val aack: AcceptAck = expectMsgPF(50 millisecond) { case a: AcceptAck => a }
      aack.requestId should be(accept.id)
      // when we send that to node zero
      actor0 ! aack
      // it commits the noop
      expectMsgPF(50 millisecond) {
        case c: Commit => // good
      }
      // then send it some data
      val hw = ClientRequestCommandValue(0, Array[Byte](1))
      actor0 ! hw
      // it will send out an accept
      val accept2 = expectMsgPF(50 millisecond) { case a: Accept => a }
      accept2.id.logIndex should be(2)
      accept2.value.asInstanceOf[ClientRequestCommandValue].bytes.length should be(1)
      accept2.id.number should be(phigh.id.number)
      // when we send that to node one
      actor1 ! accept2
      // it will ack
      val aack2 = expectMsgPF(50 millisecond) { case a: AcceptAck => a }
      aack2.requestId should be(accept2.id)
      // when we send that back to node zero
      actor0 ! aack2
      // then it responds with the committed work
      expectMsgPF(50 millisecond) {
        case b: Array[Byte] if b(0) == -1 => true
        case b => fail(s"$b")
      }
      // it will send out a commit
      val commit: Commit = expectMsgPF(50 millisecond) { case c: Commit => c }
      // when we send that to node one
      actor1 ! commit
      // and both nodes will have delivered the value
      Seq(node0, node1).map(_._map.get(2).getOrElse(fail).value) should be(Seq(hw, hw))
    }

    def `should return a response to the correct client` {
      // given node zero leader
      val node0 = new TestJournal
      val actor0 = TestActorRef(new TestPaxosActor(Configuration(InteractionSpec.config, 3), 0, self, node0, ArrayBuffer.empty, None))
      actor0.underlyingActor.setAgent(Leader, actor0.underlyingActor.data.copy(clientCommands = Map.empty, acceptResponses = SortedMap.empty, epoch = Some(BallotNumber(1, 1))))
      // and node one
      val node1 = new TestJournal
      val actor1 = TestActorRef(new TestPaxosActor(Configuration(InteractionSpec.config, 3), 1, self, node1, ArrayBuffer.empty, None))
      // different responses go back to different actors
      performConsensus(actor0, actor1, new TestProbe(system), 22)
      performConsensus(actor0, actor1, new TestProbe(system), 33)

      def performConsensus(leader: ActorRef, follower: ActorRef, client: TestProbe, msg: Byte): Unit = {
        client.send(leader, ClientRequestCommandValue(0, Array[Byte](msg)))
        // it will send out an accept
        val accept: Accept = expectMsgPF(50 millisecond) { case a: Accept => a }
        accept.value.asInstanceOf[ClientRequestCommandValue].bytes.length should be(1)
        // when we send that to node one
        follower ! accept
        // it will ack
        val aack: AcceptAck = expectMsgPF(50 millisecond) { case a: AcceptAck => a }
        aack.requestId should be(accept.id)
        // when we send that back to node zero
        leader ! aack
        // it will commit
        val commit: Commit = expectMsgPF(50 millisecond) { case c: Commit => c }
        // nothing is set back to us
        expectNoMsg(25 millisecond)
        // and response went back to the probe
        client.expectMsgPF(50 millis) {
          case bytes: Array[Byte] =>
            bytes(0) should be(-1 * msg)
        }
      }
    }

    def `should return NoLongerLeader during a failover` {
      // given node0 leader
      val node0 = new TestJournal
      val actor0 = TestActorRef(new TestPaxosActor(Configuration(InteractionSpec.config, 3), 0, self, node0, ArrayBuffer.empty, None))
      actor0.underlyingActor.setAgent(Leader, actor0.underlyingActor.data.copy(clientCommands = Map.empty, acceptResponses = SortedMap.empty, epoch = Some(BallotNumber(counter = Int.MinValue + 1, nodeIdentifier = 0))))

      // and some higher promise
      val node0progress = node0.load()
      val higherPromise = node0progress.copy(highestPromised = node0progress.highestPromised.copy(counter = node0progress.highestPromised.counter + 1, nodeIdentifier = 1))

      // and node1 which has made the higher promise
      val node1 = new TestJournal
      node1.save(higherPromise)
      val actor1 = TestActorRef(new TestPaxosActor(Configuration(InteractionSpec.config, 3), 1, self, node1, ArrayBuffer.empty, None))
      // and node2 which has made the higher promise
      val node2 = new TestJournal
      node2.save(higherPromise)
      val actor2 = TestActorRef(new TestPaxosActor(Configuration(InteractionSpec.config, 3), 2, self, node2, ArrayBuffer.empty, None))

      // when a client sends to actor0
      val client = new TestProbe(system)
      client.send(actor0, ClientRequestCommandValue(99, Array[Byte](11)))

      // it will broadcast out an accept
      val accept: Accept = expectMsgPF(100 millisecond) { case a: Accept => a }
      accept.id.number.nodeIdentifier should be(0)

      // when we send that to node1
      actor1 ! accept

      // it will nack
      val nack1: AcceptNack = expectMsgPF(100 millisecond) { case a: AcceptNack => a }
      nack1.requestId should be(accept.id)

      // we send that to the leader
      actor0 ! nack1

      // when we send the accept to node2
      actor2 ! accept

      // it will nack
      val nack2: AcceptNack = expectMsgPF(100 millisecond) { case a: AcceptNack => a }
      nack2.requestId should be(accept.id)

      // we send that to the leader
      actor0 ! nack2

      // nothing is set back to us
      expectNoMsg(25 millisecond)

      // and it told the client it had lost leadership
      client.expectMsgPF(100 millis) {
        case nlle: NoLongerLeaderException if nlle.msgId == 99 => // good
        case x => fail(s"unexpected msg $x")
      }
    }

    def `should widen the recovery slot range if discovers other nodes have seen higher slots`: Unit = {
      expectNoMsg(25 millisecond)
      // given node zero
      val journal0 = new TestJournal
      val actor0 = TestActorRef(new TestPaxosActor(Configuration(InteractionSpec.config, 3), 0, self, journal0, ArrayBuffer.empty, None))
      // and node one with a three accepted values but no committed
      val journal1 = new TestJournal
      val v1 = ClientRequestCommandValue(11, Array[Byte] {
        0
      })
      val v2 = ClientRequestCommandValue(22, Array[Byte] {
        1
      })
      val v3 = ClientRequestCommandValue(22, Array[Byte] {
        3
      })
      val a1 = Accept(Identifier(0, BallotNumber(Int.MinValue + 1, Int.MinValue + 1), 1L), v1)
      val a2 = Accept(Identifier(0, BallotNumber(Int.MinValue + 1, Int.MinValue + 1), 2L), v2)
      val a3 = Accept(Identifier(0, BallotNumber(Int.MinValue + 1, Int.MinValue + 1), 3L), v3)
      journal1.accept(Seq(a1, a2, a3): _*)
      val actor1 = TestActorRef(new TestPaxosActor(Configuration(InteractionSpec.config, 3), 1, self, journal1, ArrayBuffer.empty, None))
      // when node zero times-out
      actor0 ! CheckTimeout
      // it issues a low prepare
      expectMsg(50 millisecond, minPrepare)
      // and node one will nack the load prepare
      actor1 ! minPrepare
      val nack: PrepareNack = expectMsgPF(50 millisecond) { case p: PrepareNack => p }
      // which will cause node zero to issue a higher prepare
      nack.requestId.from shouldBe 0
      // when we send it back to node zero
      actor0 ! nack
      // it issues a higher prepare
      val phigh1: Prepare = expectMsgPF(50 milliseconds) { case hprepare: Prepare => hprepare }
      phigh1.id.logIndex shouldBe 1
      phigh1.id.number.nodeIdentifier shouldBe 0
      // when we send that high prepare to node one
      actor1 ! phigh1
      // it should ack
      val pack1 = expectMsgPF(50 millisecond) { case p: PrepareAck => p }
      pack1.requestId should be(phigh1.id)
      pack1.highestAcceptedIndex shouldBe 3L

      // when we respond back to node zero
      actor0 ! pack1

      // node zero will then ask about slots 2 and 3
      val phigh2: Prepare = expectMsgPF(50 milliseconds) { case hprepare: Prepare => hprepare }
      phigh2.id.logIndex shouldBe 2
      phigh2.id.number.nodeIdentifier shouldBe 0
      val phigh3: Prepare = expectMsgPF(50 milliseconds) { case hprepare: Prepare => hprepare }
      phigh3.id.logIndex shouldBe 3
      phigh3.id.number.nodeIdentifier shouldBe 0

      // and will issue an accept for slot 1 giving the value returned by node1
      val accept1: Accept = expectMsgPF(50 millisecond) { case a: Accept => a }
      accept1.id.logIndex shouldBe 1
      accept1.value shouldBe v1
      accept1.id.number should be(phigh1.id.number)
      // and ack its own accept
      actor0.underlyingActor.data.acceptResponses match {
        case map if map.nonEmpty =>
          map.get(accept1.id) match {
            case None => fail()
            case Some(AcceptResponsesAndTimeout(_, _, responses)) =>
              responses.values.headOption match {
                case Some(a: AcceptAck) => //good
                case x => fail(x.toString)
              }
          }
        case _ => fail()
      }

      // when we send node1 the slot 2&3 high prepares it will ack
      actor1 ! phigh2
      val pack2 = expectMsgPF(50 millisecond) { case p: PrepareAck => p }
      pack2.requestId shouldBe (phigh2.id)
      actor1 ! phigh3
      val pack3 = expectMsgPF(50 millisecond) { case p: PrepareAck => p }
      pack3.requestId shouldBe phigh3.id

      // when we send node0 the ack for slot2
      actor0 ! pack2
      // it will issue an accept
      val accept2: Accept = expectMsgPF(50 millisecond) { case a: Accept => a }
      accept2.id.logIndex shouldBe 2
      accept2.value shouldBe v2
      accept2.id.number shouldBe phigh2.id.number
      // and ack its own accept
      actor0.underlyingActor.data.acceptResponses match {
        case map if map.nonEmpty =>
          map.get(accept2.id) match {
            case Some(AcceptResponsesAndTimeout(_, _, responses)) =>
              responses.values.headOption match {
                case Some(a: AcceptAck) => //good
                case x => fail(x.toString)
              }
            case x => fail(x.toString)
          }
        case _ => fail()
      }
      // when we send node0 the ack for slot3
      actor0 ! pack3
      // it will issue an accept
      val accept3: Accept = expectMsgPF(50 millisecond) { case a: Accept => a }
      accept3.id.logIndex shouldBe 3
      accept3.value shouldBe v3
      accept3.id.number shouldBe phigh2.id.number
      // and ack its own accept
      actor0.underlyingActor.data.acceptResponses match {
        case map if map.nonEmpty =>
          map.get(accept3.id) match {
            case None => fail()
            case Some(AcceptResponsesAndTimeout(_, _, responses)) =>
              responses.values.headOption match {
                case Some(a: AcceptAck) => //good
                case x => fail(x.toString)
              }
          }
        case _ => fail()
      }

      // when we send accept1 to node1 it will ack and node0 will commit
      actor1 ! accept1
      val aack1: AcceptAck = expectMsgPF(50 millisecond) { case a: AcceptAck => a }
      aack1.requestId should be(accept1.id)
      // when we send that to node zero
      actor0 ! aack1
      // it commits
      expectMsgPF(50 millisecond) {
        case c: Commit => // good
      }

      // when we send accept1 to node1 it will ack and node0 will commit
      actor1 ! accept2
      val aack2: AcceptAck = expectMsgPF(50 millisecond) { case a: AcceptAck => a }
      aack2.requestId shouldBe (accept2.id)
      // when we send that to node zero
      actor0 ! aack2
      // it commits
      expectMsgPF(50 millisecond) {
        case c: Commit => // good
      }
      // when we send accept1 to node1 it will ack and node0 will commit
      actor1 ! accept3
      val aack3: AcceptAck = expectMsgPF(50 millisecond) { case a: AcceptAck => a }
      aack3.requestId shouldBe accept3.id
      // when we send that to node zero
      actor0 ! aack3
      // it commits
      expectMsgPF(50 millisecond) {
        case c: Commit => // good
      }
    }
  }
}