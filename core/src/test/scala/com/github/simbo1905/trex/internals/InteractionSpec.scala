package com.github.simbo1905.trex.internals

import akka.testkit.TestKit
import org.scalatest.SpecLike
import com.typesafe.config.ConfigFactory
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.ImplicitSender
import org.scalatest.BeforeAndAfterAll
import com.github.simbo1905.trex.Journal
import scala.collection.SortedMap
import scala.collection.immutable.TreeMap
import akka.testkit.TestFSMRef
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import com.github.simbo1905.trex._
import PaxosActor._
import org.scalatest.Matchers
import akka.testkit.TestProbe
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
      val actor0 = TestFSMRef(new TestPaxosActor(Configuration(InteractionSpec.config, 3), 0, self, node0, ArrayBuffer.empty, None))
      // and node one
      val node1 = new TestJournal
      val actor1 = TestFSMRef(new TestPaxosActor(Configuration(InteractionSpec.config, 3), 1, self, node1, ArrayBuffer.empty, None))
      // when node zero times-out
      actor0 ! PaxosActor.CheckTimeout
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
      actor0.stateData.acceptResponses match {
        case map if map.nonEmpty =>
          map.get(accept.id) match {
            case None => fail()
            case Some(m) =>
              m.get.values.head match {
                case a: AcceptAck => //good
                case b: AcceptNack => fail()
              }
          }
        case _ => fail()
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
      // it will commit
      val commit: Commit = expectMsgPF(50 millisecond) { case c: Commit => c }
      // when we send that to node one
      actor1 ! commit
      // then it responds with the committed work
      expectMsgPF(50 millisecond) {
        case b: Array[Byte] if b(0) == -1 => true
      }
      // and both nodes will have delivered the value
      Seq(node0, node1).map(_._map.get(2).get.value) should be(Seq(hw, hw))
    }

    def `should return a response to the correct client` {
      // given node zero leader
      val node0 = new TestJournal
      val actor0 = TestFSMRef(new TestPaxosActor(Configuration(InteractionSpec.config, 3), 0, self, node0, ArrayBuffer.empty, None))
      actor0.setState(Leader, actor0.stateData.copy(clientCommands = Map.empty, acceptResponses = SortedMap.empty, epoch = Some(BallotNumber(1, 1))))
      // and node one
      val node1 = new TestJournal
      val actor1 = TestFSMRef(new TestPaxosActor(Configuration(InteractionSpec.config, 3), 1, self, node1, ArrayBuffer.empty, None))
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
      val actor0 = TestFSMRef(new TestPaxosActor(Configuration(InteractionSpec.config, 3), 0, self, node0, ArrayBuffer.empty, None))
      actor0.setState(Leader, actor0.stateData.copy(clientCommands = Map.empty, acceptResponses = SortedMap.empty, epoch = Some(BallotNumber(counter = Int.MinValue + 1, nodeIdentifier = 0))))

      // and some higher promise
      val node0progress = node0.load()
      val higherPromise = node0progress.copy(highestPromised = node0progress.highestPromised.copy(counter = node0progress.highestPromised.counter + 1, nodeIdentifier = 1))

      // and node1 which has made the higher promise
      val node1 = new TestJournal
      node1.save(higherPromise)
      val actor1 = TestFSMRef(new TestPaxosActor(Configuration(InteractionSpec.config, 3), 1, self, node1, ArrayBuffer.empty, None))
      // and node2 which has made the higher promise
      val node2 = new TestJournal
      node2.save(higherPromise)
      val actor2 = TestFSMRef(new TestPaxosActor(Configuration(InteractionSpec.config, 3), 2, self, node2, ArrayBuffer.empty, None))

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
      }
    }

    // FIXME a widening of the slot range test
  }

}