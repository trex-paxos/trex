package com.github.simbo1905.trex.library

import com.github.simbo1905.trex.library.TestHelpers._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{OptionValues, Spec}

import scala.collection.immutable.TreeMap
import scala.compat.Platform

import Ordering._

class TestPrepareHandler extends PrepareHandler

class PrepareHandlerTests extends Spec with MockFactory with OptionValues {
  val agentPromise10 = PaxosAgent(0, Follower, initialData.copy(progress = initialData.progress.copy(highestPromised = BallotNumber(10, 10))))

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
      io.sent.headOption.value match {
        case MessageAndTimestamp(ack: PrepareAck, sendTs) =>
          journal.p.get() match {
            case (saveTs, _ ) if saveTs < sendTs => // good
              println(s"$saveTs < $sendTs")
            case f => fail(f.toString)
          }
        case x => fail(x.toString)
      }
    }

    def `should have the new promise in the output agent data` {
      val mockJournal = stub[Journal]
      (mockJournal.save _ ).when(*)
      (mockJournal.bounds _).when().returns(JournalBounds(0,0))
      val io = new TestIO(mockJournal)
      testPromiseHandler.handlePrepare(io, agentPromise10, Prepare(Identifier(0, BallotNumber(11, 11), 10))) match {
        case PaxosAgent(_, _, data) if data.progress.highestPromised == BallotNumber(11, 11) => // good
        case x => fail(x.toString)
      }
    }

    def `should clear any none follower data and become follower when recoverer` = {
      val id = Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), 0)
      val recoverLikeData = initialData.copy(epoch = Some(BallotNumber(10, 10)),
        prepareResponses = TreeMap(id -> Map.empty),
        acceptResponses = emptyAcceptResponses98)
      val mockJournal = stub[Journal]
      (mockJournal.save _ ).when(*)
      (mockJournal.bounds _).when().returns(JournalBounds(0,0))
      val io = new TestIO(mockJournal)
      testPromiseHandler.handlePrepare(io, agentPromise10.copy(data = recoverLikeData, role = Recoverer), Prepare(Identifier(0, BallotNumber(11, 11), 10))) match {
        case PaxosAgent(_, _, data) if data.progress.highestPromised == BallotNumber(11, 11) =>
          data match {
            case p if p.epoch == None && p.acceptResponses.isEmpty && p.prepareResponses.isEmpty && p.clientCommands.isEmpty => // good
            case x =>
              fail(x.toString)
          }
        case x => fail(x.toString)
      }
    }

    def `should send out NotLeader and return to follower if leader` {
      val id = Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), 0)
      val clientCommands: Map[Identifier, (CommandValue, String)] = Map(id -> (NoOperationCommandValue -> DummyRemoteRef()))
      val clientCommandsData = initialData.copy(clientCommands = clientCommands)
      val mockJournal = stub[Journal]
      (mockJournal.save _ ).when(*)
      (mockJournal.bounds _).when().returns(JournalBounds(0,0))
      var sentNoLongerLeader = false
      val io = new TestIO(mockJournal) {
        override def sendNoLongerLeader(cmds: Map[Identifier, (CommandValue, String)]): Unit = {
          sentNoLongerLeader = true
          cmds match {
            case `clientCommands` => // good
            case x => fail(x.toString())
          }
        }
      }
      testPromiseHandler.handlePrepare(io, agentPromise10.copy(data = clientCommandsData, role = Leader), Prepare(Identifier(0, BallotNumber(11, 11), 10)))
      assert(sentNoLongerLeader)
    }

    def `should nack lower number` {
      val mockJournal = stub[Journal]
      (mockJournal.bounds _).when().returns(JournalBounds(0,0))
      (mockJournal.accepted _).when(*).returns(None)
      val io = new TestIO(mockJournal)
      testPromiseHandler.handlePrepare(io, agentPromise10, Prepare(Identifier(0, BallotNumber(9, 9), 10)))
      io.sent.headOption.value match {
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
      io.sent.headOption.value match {
        case MessageAndTimestamp(ack: PrepareAck, _) => // good
        case x => fail(x.toString)
      }
    }
  }

}
