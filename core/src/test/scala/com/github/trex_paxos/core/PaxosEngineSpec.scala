package com.github.trex_paxos.core

import com.github.trex_paxos.{PaxosProperties}
import com.github.trex_paxos.library._
import org.scalatest.{AsyncWordSpec, Matchers, OptionValues}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

class PaxosEngineSpec extends AsyncWordSpec with Matchers with OptionValues {

  val nodeUniqueId: Int = 0

  val zeroProgress = Progress(
    highestPromised = BallotNumber(0, nodeUniqueId),
    highestCommitted = Identifier(from = 0, number = BallotNumber(0, 0), logIndex = 0)
  )

  def testEngine: TestPaxosEngine = {
    val testJournal = new TestJournal
    new TestPaxosEngine(PaxosProperties(0L, 100L), testJournal, PaxosEngine.initialAgent(nodeUniqueId, zeroProgress, () => 3)) {
      override def deliver(payload: Payload): Any = {}

      override def associate(value: CommandValue, id: Identifier): Unit = {}

      override def respond(results: Option[Map[Identifier, Any]]): Unit = {}
    }
  }

  "PaxosEngine" should {
//    "should respond with a nack for a low prepare" in {
//      val testPaxosEngine = testEngine
//
//      val id = Identifier(1, BallotNumber(-1, -1), 0)
//      val nack = PrepareNack(id, nodeUniqueId, zeroProgress, 0, 0)
//
//      for {
//        seq <- testPaxosEngine ? Prepare(id)
//      } yield {
//        assert( seq.size == 1 && seq.headOption.value == nack)
//      }
//    }
//    "should respond with two successive nacks when it sees two successive low prepares" in {
//      val testPaxosEngine = testEngine
//
//      val id = Identifier(1, BallotNumber(-1, -1), 0)
//      val id2 = Identifier(2, BallotNumber(-2, -2), 0)
//
//      val n1 = PrepareNack(id, nodeUniqueId, zeroProgress, 0, 0)
//      val n2 = PrepareNack(id2, nodeUniqueId, zeroProgress, 0, 0)
//
//      for {
//        f1 <- testPaxosEngine ? Prepare(id)
//        f2 <- testPaxosEngine ? Prepare(id2)
//      } yield {
//        assert( f1.size == 1 && f2.size == 1 && f1.headOption.value == n1 && f2.headOption.value == n2)
//      }
//    }
//    "deliver on Commit" in {
//      val stubJournal: Journal = new UndefinedJournal {
//        override def saveProgress(progress: Progress): Unit = ()
//
//        override def accepted(logIndex: Long): Option[Accept] = TestHelpers.journaled11thru14(logIndex)
//      }
//      // some progress ready to commit
//      val oldProgress = Progress(TestHelpers.a12.id.number, TestHelpers.a11.id)
//      // and an agent
//      val agent = PaxosAgent(0, Follower, TestHelpers.initialData.copy(progress = oldProgress), TestHelpers.initialQuorumStrategy)
//      // and something to collect messages
//      val msgs = ArrayBuffer[Payload]()
//
//      val testPaxosEngine = new TestPaxosEngine(PaxosProperties(1000L, 3000L), stubJournal, agent) {
//        override def deliver(payload: Payload): Any = {
//          msgs += payload
//          msgs.synchronized {
//            msgs.notifyAll()
//          }
//          "cool!"
//        }
//
//        override def associate(value: CommandValue, id: Identifier): Unit = {}
//
//        override def respond(results: Option[Map[Identifier, Any]]): Unit = {}
//      }
//
//      testPaxosEngine.receive(Commit(TestHelpers.a12.id, TestHelpers.initialData.leaderHeartbeat))
//
//      Future {
//        msgs.synchronized {
//          if( msgs.isEmpty ){
//            msgs.wait(250L)
//          }
//        }
//        val twelve = msgs filter {
//          case Payload(12, _) => true
//          case _ => false
//        }
//        assert(twelve.nonEmpty, s"12 not in $msgs")
//      }
//    }
  }
}
