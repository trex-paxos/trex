package com.github.trex_paxos.internals

import akka.actor.ActorSystem
import akka.testkit.{DefaultTimeout, ImplicitSender, TestActorRef, TestKit}
import com.github.trex_paxos.internals.AllStateSpec._
import com.github.trex_paxos.internals.PaxosActor.Configuration
import com.github.trex_paxos.library._
import com.typesafe.config.ConfigFactory
import org.scalamock.scalatest.MockFactory
import org.scalatest._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.postfixOps

object MiscellaneousTests {
  val config = ConfigFactory.parseString("trex.leader-timeout-min=10\ntrex.leader-timeout-max=20\nakka.loglevel = \"DEBUG\"")
  val minPrepare = Prepare(Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), Long.MinValue))
}

class MiscellaneousTests
  extends TestKit(ActorSystem("MiscellaneousTests", MiscellaneousTests.config))
  with DefaultTimeout with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfter with MockFactory with OptionValues {

  import MiscellaneousTests._

  "Follower" should {

    "bootstrap from retransmit response" in {
      // given some retransmitted committed values
      val v1 = ClientRequestCommandValue(0, Array[Byte] {0})
      val v2 = ClientRequestCommandValue(1, Array[Byte] {1})
      val v3 = ClientRequestCommandValue(2, Array[Byte] {2})
      val a1 =
        Accept(Identifier(1, BallotNumber(1, 1), 1L), v1)
      val a2 =
        Accept(Identifier(2, BallotNumber(2, 2), 2L), v2)
      val a3 =
        Accept(Identifier(3, BallotNumber(3, 3), 3L), v3)
      val retransmission = RetransmitResponse(1, 0, Seq(a1, a2, a3), Seq.empty)

      // and an empty node
      val fileJournal: FileJournal = AllStateSpec.tempRecordTimesFileJournal
      val delivered = ArrayBuffer[CommandValue] ()
      val fsm = TestActorRef(new TestPaxosActor(Configuration(config, 3), 0, self, fileJournal, delivered, None))

      // when the retransmission is received
      fsm ! retransmission

      // it sends no messages
      expectNoMsg(25 milliseconds)
      // stays in state
      assert(fsm.underlyingActor.role == Follower)
      // updates its commit index
      assert(fsm.underlyingActor.data.progress.highestCommitted == a3.id)

      // delivered the committed values
      delivered.size should be(3)
      delivered(0) should be(v1)
      delivered(1) should be(v2)
      delivered(2) should be(v3)

      // journaled the values so that it can retransmit itself
      fileJournal.bounds should be(JournalBounds(1, 3))
      fileJournal.accepted(1) match {
        case Some(a) if a.id == a1.id => // good
        case x => fail(x.toString)
      }
      fileJournal.accepted(2) match {
        case Some(a) if a.id == a2.id => // good
        case x => fail(x.toString)
      }
      fileJournal.accepted(3) match {
        case Some(a) if a.id == a3.id => // good
        case x => fail(x.toString)
      }

      // and journal the new progress
      fileJournal.load() match {
        case Progress(_, a3.id) => // good
        case p => fail(s"got $p not ${
          Progress(a3.id.number, a3.id)
        }")
      }
    }

    "switch to recoverer and issue multiple prepare messages if there are slots to recover and no leader" in {
      // given that we control the clock
      val timenow = 999L
      var saveTime = 0L
      // and a journal which records the save time
      val stubJournal: Journal = stub[Journal]
      val delegatingJournal = new DelegatingJournal(stubJournal) {
        override def save(progress: Progress): Unit = {
          saveTime = System.nanoTime()
          super.save(progress)
        }
      }

      // and that we record the send time
      var sendTime = 0L
      val fsm = TestActorRef(new TestPaxosActor(Configuration(config, 3), 0, self, delegatingJournal, ArrayBuffer.empty, None) {
        override def clock() = timenow

        override def broadcast(msg: PaxosMessage): Unit = {
          sendTime = System.nanoTime()
          super.broadcast(msg)
        }
      })
      fsm.underlyingActor.setAgent(Follower, initialData)

      // and three uncommitted values in journal
      val bounds = JournalBounds(0L, 3L)
      (stubJournal.bounds _) when() returns (bounds)

      val id1 = Identifier(0, BallotNumber(lowValue + 1, 0), 1)
      (stubJournal.accepted _) when (1L) returns Some(Accept(id1, ClientRequestCommandValue(0, expectedBytes)))

      val id2 = Identifier(0, BallotNumber(lowValue + 1, 0), 2)
      (stubJournal.accepted _) when (2L) returns Some(Accept(id2, ClientRequestCommandValue(0, expectedBytes)))

      val id3 = Identifier(0, BallotNumber(lowValue + 1, 0), 3)
      (stubJournal.accepted _) when (3L) returns Some(Accept(id3, ClientRequestCommandValue(0, expectedBytes)))

      val id4 = Identifier(0, BallotNumber(lowValue + 1, 0), 4)

      // when our node gets a timeout
      fsm ! CheckTimeout

      // it sends out a single low prepare
      expectMsg(100 millisecond, minPrepare)

      // when it hears back that the other follower does not have a fresh heartbeat
      fsm ! PrepareNack(minPrepare.id, 2, initialData.progress, initialData.progress.highestCommitted.logIndex, initialData.leaderHeartbeat)

      // it sends out four prepares for each uncommitted slot plus an extra slot
      val prepare1 = Prepare(id1)
      expectMsg(100 millisecond, prepare1)

      val prepare2 = Prepare(id2)
      expectMsg(100 millisecond, prepare2)

      val prepare3 = Prepare(id3)
      expectMsg(100 millisecond, prepare3)

      val prepare4 = Prepare(id4)
      expectMsg(100 millisecond, prepare4)

      // and promotes to candidate
      assert(fsm.underlyingActor.role == Recoverer)
      // and sets a fresh timeout
      assert(fsm.underlyingActor.data.timeout > 0 && fsm.underlyingActor.data.timeout - timenow < config.getLong(PaxosActor.leaderTimeoutMaxKey))
      // and make a promise to self
      assert(fsm.underlyingActor.data.progress.highestPromised == prepare1.id.number)
      // and votes for its own prepares
      assert(!fsm.underlyingActor.data.prepareResponses.isEmpty)
      val prapareIds = fsm.underlyingActor.data.prepareResponses map {
        case (id, map) if map.keys.headOption == Some(0) && map.values.headOption.getOrElse(fail).requestId == id =>
          id
        case x => fail(x.toString)
      }
      assert(true == prapareIds.toSet.contains(prepare1.id))
      assert(true == prapareIds.toSet.contains(prepare2.id))
      assert(true == prapareIds.toSet.contains(prepare3.id))

      // and has no accepts
      assert(fsm.underlyingActor.data.acceptResponses.isEmpty)

      // and it sent out the messages only after having journalled its own promise
      assert(saveTime != 0)
      assert(sendTime != 0)
      assert(saveTime < sendTime)
    }
  }

}
