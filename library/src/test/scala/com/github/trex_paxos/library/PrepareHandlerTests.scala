package com.github.trex_paxos.library

import TestHelpers._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{OptionValues}

import scala.collection.immutable.TreeMap
import Ordering._
import org.scalatest.refspec.RefSpec

class TestPrepareHandler extends PrepareHandler

class PrepareHandlerTests extends RefSpec with MockFactory with OptionValues {
  val agentPromise10 = PaxosAgent(0, Follower, initialData.copy(progress = initialData.progress.copy(highestPromised = BallotNumber(10, 10))), initialQuorumStrategy)

  object `A PromiseHandler` {

    val testPromiseHandler = new TestPrepareHandler

    def `high perpare logic should require that the prepare number is higher than the current promise` {
      intercept[IllegalArgumentException] {
        testPromiseHandler.handleHighPrepare(undefinedIO, agentPromise10, Prepare(Identifier(0, BallotNumber(1, 1), 10)))
      }
    }

    def `should respond with an ack and journal before sending` {
      val prepare = Prepare(Identifier(0, BallotNumber(11, 11), 10))
      val journal = new InMemoryJournal
      val io = new TestIO(journal)
      testPromiseHandler.handlePrepare(io, agentPromise10, prepare)
      io.sent().headOption.value match {
        case MessageAndTimestamp(ack: PrepareAck, sendTs) =>
          journal.p() match {
            case (saveTs, _ ) if saveTs < sendTs => // good
            case f => fail(f.toString)
          }
        case x => fail(x.toString)
      }
    }

    def `should have the new promise in the output agent data` {
      val mockJournal = stub[Journal]
      (mockJournal.saveProgress _ ).when(*)
      (mockJournal.bounds _).when().returns(JournalBounds(0,0))
      val io = new TestIO(mockJournal)
      testPromiseHandler.handlePrepare(io, agentPromise10, Prepare(Identifier(0, BallotNumber(11, 11), 10))) match {
        case PaxosAgent(_, _, data, _) if data.progress.highestPromised == BallotNumber(11, 11) => // good
        case x => fail(x.toString)
      }
    }

    def `should clear any none follower data and become follower when recoverer` = {
      val id = Identifier(0, BallotNumber(0, 0), 0)
      val recoverLikeData = initialData.copy(epoch = Some(BallotNumber(10, 10)),
        prepareResponses = TreeMap(id -> Map.empty),
        acceptResponses = emptyAcceptResponses98)
      val mockJournal = stub[Journal]
      (mockJournal.saveProgress _ ).when(*)
      (mockJournal.bounds _).when().returns(JournalBounds(0,0))
      val io = new TestIO(mockJournal){}
      testPromiseHandler.handlePrepare(io, agentPromise10.copy(data = recoverLikeData, role = Recoverer), Prepare(Identifier(0, BallotNumber(11, 11), 10))) match {
        case PaxosAgent(_, _, data, _) if data.progress.highestPromised == BallotNumber(11, 11) =>
          data match {
            case p if p.epoch == None && p.acceptResponses.isEmpty && p.prepareResponses.isEmpty => // good
            case x =>
              fail(x.toString)
          }
        case x => fail(x.toString)
      }
    }

    def `should send out NotLeader and return to follower if leader` {
      val id = Identifier(0, BallotNumber(0, 0), 0)
      val mockJournal = stub[Journal]
      (mockJournal.saveProgress _ ).when(*)
      (mockJournal.bounds _).when().returns(JournalBounds(0,0))
      val sentNoLongerLeader = Box(false)
      val io = new TestIO(mockJournal) {
        override def respond(results: Option[Map[Identifier, Any]]): Unit = results match {
          case None => sentNoLongerLeader(true)
          case f => fail(f.toString)
        }
      }
      testPromiseHandler.handlePrepare(io, agentPromise10.copy(role = Leader), Prepare(Identifier(0, BallotNumber(11, 11), 10)))
      assert(sentNoLongerLeader())
    }

    def `should nack lower number` {
      val mockJournal = stub[Journal]
      (mockJournal.bounds _).when().returns(JournalBounds(0,0))
      (mockJournal.accepted _).when(*).returns(None)
      val io = new TestIO(mockJournal)
      testPromiseHandler.handlePrepare(io, agentPromise10, Prepare(Identifier(0, BallotNumber(9, 9), 10)))
      io.sent().headOption.value match {
        case MessageAndTimestamp(ack: PrepareNack, _) => // good
        case x => fail(x.toString)
      }
    }

    def `should ack equal number` {
      val mockJournal = stub[Journal]
      (mockJournal.bounds _).when().returns(JournalBounds(0,0))
      (mockJournal.accepted _).when(*).returns(None)
      val io = new TestIO(mockJournal)
      testPromiseHandler.handlePrepare(io, agentPromise10, Prepare(Identifier(0, BallotNumber(10, 10), 10)))
      io.sent().headOption.value match {
        case MessageAndTimestamp(ack: PrepareAck, _) => // good
        case x => fail(x.toString)
      }
    }
  }

}
