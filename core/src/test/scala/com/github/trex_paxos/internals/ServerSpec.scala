package com.github.trex_paxos.internals

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import _root_.com.github.trex_paxos.TrexRouting
import _root_.com.github.trex_paxos.library.{RetransmitRequest, RetransmitResponse, _}
import org.scalatest.refspec.RefSpecLike

import org.scalatest._
import matchers.should._


import scala.concurrent.duration._
import scala.language.postfixOps

object ServerSpec {
  val config = _root_.com.typesafe.config.ConfigFactory.parseString("trex.leader-timeout-min=1\ntrex.leader-timeout-max=10\nakka.loglevel = \"DEBUG\"")
}

class ServerSpec extends TestKit(ActorSystem("ServerSpec", ServerSpec.config))
  with RefSpecLike with ImplicitSender with BeforeAndAfterAll with Matchers {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  object `member pattern` {
    def `will parse string`: Unit = {
      import Member.pattern
      val pattern(host, port) = "localhost:1234"
      host shouldBe "localhost"
      port shouldBe "1234"
      val pattern(host2, port2) = "127.0.0.1:9876"
      host2 shouldBe "127.0.0.1"
      port2 shouldBe "9876"
    }
  }

  object `trex routing` {

    def testableRouting = {
      val listener = new TestProbe(system)

      val peer1 = new TestProbe(system)
      val peer2 = new TestProbe(system)
      val thePeers = Map(1 -> peer1, 2 -> peer2)

      val target = new TestProbe(system)

      val routing = TestActorRef(new TrexRouting {
        override def networkListener: ActorRef = listener.ref

        override def paxosActor: ActorRef = target.ref

        override def peers: Map[Int, ActorRef] = thePeers.mapValues(_.ref)
      })

      (routing, listener, target, thePeers)

    }

    def `will route inbound to target`: Unit = {
      // given a router to test
      val (routing, listener, target, peers) = testableRouting
      // when the network listener sends a message to the router
      listener.send(routing, "hello")
      // then the target gets the message and no-one else
      target.expectMsg("hello")
      peers.values.foreach(_.expectNoMessage(50 millisecond))
      listener.expectNoMessage(50 millisecond)
    }

    def `will broadcast outbound from target`: Unit = {
      // given a router to test
      val (routing, listener, target, peers) = testableRouting
      // and a test message
      val msg = Commit(Identifier(0, BallotNumber(0, 0), 0L))
      // when the target sends a message to the router
      target.send(routing, msg)
      // then the peers gets the message but no-one else
      peers.values.foreach(_.expectMsg(msg))
      target.expectNoMessage(50 millisecond)
      listener.expectNoMessage(50 millisecond)
    }

    def `will route outbound AcceptResponse from target to peer 1`: Unit = {
      // given a router to test
      val msg = AcceptAck(Identifier(from = 1, BallotNumber(2, 3), 4L), 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)))
      checkPeer(msg, 1)
    }

    def `will route outbound AcceptResponse from target to peer 2`: Unit = {
      // given a router to test
      val msg = AcceptAck(Identifier(from = 2, BallotNumber(2, 3), 4L), 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)))
      checkPeer(msg, 2)
    }

    def `will route outbound AcceptResponse Nack from target to peer 2`: Unit = {
      // given a router to test
      val msg = AcceptNack(Identifier(from = 2, BallotNumber(2, 3), 4L), 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)))
      checkPeer(msg, 2)
    }

    def `will route outbound PrepareResponse from target to peer 1`: Unit = {
      // given a router to test
      val msg = PrepareAck(Identifier(from = 1, BallotNumber(2, 3), 4L), 5,
        Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), 0, 0, None)
      checkPeer(msg, 1)
    }

    def `will route outbound PrepareResponse from target to peer 2`: Unit = {
      // given a router to test
      val msg = PrepareAck(Identifier(from = 2, BallotNumber(2, 3), 4L), 5,
        Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), 0, 0, None)
      checkPeer(msg, 2)
    }

    def `will route outbound PrepareResponse Nack from target to peer 2`: Unit = {
      // given a router to test
      val msg = PrepareNack(Identifier(from = 2, BallotNumber(2, 3), 4L), 5,
        Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), 0, 0)
      checkPeer(msg, 2)
    }

    def `will route outbound RetransmitResponse from target to peer 1`: Unit = {
      // given a router to test
      val msg = RetransmitResponse(from = 0, to = 1, Seq(), Seq())
      checkPeer(msg, 1)
    }

    def `will route outbound RetransmitResponse from target to peer 2`: Unit = {
      // given a router to test
      val msg = RetransmitResponse(from = 0, to = 2, Seq(), Seq())
      checkPeer(msg, 2)
    }

    def `will route outbound RetransmitRequest from target to peer 1`: Unit = {
      // given a router to test
      val msg = RetransmitRequest(from = 0, to = 1, 0)
      checkPeer(msg, 1)
    }

    def `will route outbound RetransmitRequest from target to peer 2`: Unit = {
      // given a router to test
      val msg = RetransmitRequest(from = 0, to = 2, 0)
      checkPeer(msg, 2)
    }

    def checkPeer(msg: AnyRef, peer: Int): Unit = {
      val (routing, listener, target, peers) = testableRouting
      // and a test message from peer 1
      // when the target sends a message to the router
      target.send(routing, msg)
      // then the peers gets the message but no-one else
      peers.foreach {
        case (n, p) if n == peer =>
          p.expectMsg(msg)
        case (n, p) if n != peer =>
          p.expectNoMessage(50 millisecond)
      }
      target.expectNoMessage(50 millisecond)
      listener.expectNoMessage(50 millisecond)
    }
  }

}
