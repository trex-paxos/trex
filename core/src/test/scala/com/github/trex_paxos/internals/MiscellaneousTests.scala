package com.github.trex_paxos.internals

import akka.actor.ActorSystem
import akka.testkit.{DefaultTimeout, ImplicitSender, TestActorRef, TestKit}
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
  }

}
