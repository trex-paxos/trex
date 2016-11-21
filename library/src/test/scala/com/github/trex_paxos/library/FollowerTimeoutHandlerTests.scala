package com.github.trex_paxos.library

import org.scalatest.{Matchers, OptionValues, WordSpecLike}

import scala.collection.immutable.TreeMap
import TestHelpers._

import Ordering._

import scala.collection.mutable.ArrayBuffer

object TestFollowerHandler extends PaxosLenses {


  val minPrepare: Prepare = Prepare(Identifier(99, BallotNumber(Int.MinValue, Int.MinValue), Long.MinValue))

  val selfNack = PrepareNack(minPrepare.id, 0, initialData.progress, 0, 999)

  val initialDataWithTimeoutAndPrepareResponses =
    timeoutPrepareResponsesLens.set(initialData, (99L, TreeMap(minPrepare.id -> Map(0 -> selfNack))))
}

class TestFollowerHandler extends FollowerHandler with BackdownAgent {
  override def highestAcceptedIndex(io: PaxosIO): Long = 0L
}

class FollowerTimeoutHandlerTests extends WordSpecLike with Matchers with OptionValues {

  import TestFollowerHandler._

  "FollowerTimeoutHandler" should {
    "broadcast a minPrepare and set a new timeout" in {
      // given a handler which records what it broadcasts
      val sentMsg: Box[Any] = new Box(None)
      val handler = new TestFollowerHandler
      val agent: PaxosAgent = PaxosAgent(99, Follower, initialData, initialQuorumStrategy)
      // when timeout on minPrepare logic is invoked
      handler.sendLowPrepares(new TestIO(new UndefinedJournal){
        override def send(msg: PaxosMessage): Unit = sentMsg(msg)
        override def randomTimeout: Long = 12345L
      }, agent) match {
        case PaxosAgent(99, Follower, data: PaxosData, _) =>
          // it should set a fresh timeout
          data.timeout shouldBe 12345L
          // and have nacked its own minPrepare
          data.prepareResponses.getOrElse(minPrepare.id, Map.empty).get(99) match {
            case Some(nack: PrepareNack) => // good
            case x => fail(x.toString)
          }
        case x => fail(x.toString)
      }
      sentMsg() shouldBe minPrepare
    }
    "rebroadcast minPrepare and set a new timeout" in {
      // given a handler which records what it broadcasts
      val sentMsg: Box[Any] = new Box(None)
      val handler = new TestFollowerHandler
      // when timeout on minPrepare logic is invoked
      handler.handleFollowerResendLowPrepares(new TestIO(new UndefinedJournal){
        override def send(msg: PaxosMessage): Unit = sentMsg(msg)
        override def randomTimeout: Long = 12345L
      }, PaxosAgent(99, Follower, initialData, initialQuorumStrategy)) match {
        case PaxosAgent(_, _, data, _) =>
          // it should set a fresh timeout
          data.timeout shouldBe 12345L
        case f => fail(f.toString)
      }
      // and have broadcast the minPrepare
      sentMsg() match {
        case `minPrepare` => // good
        case x => fail(x.toString)
      }
    }
    "backdown and send retransmit request if sees evidence of higher committed slot in response" in {
      // given a handler which records what it sends and that it has backed down
      val sentMsg: Box[Any] = new Box(None)
      val handler = new TestFollowerHandler
      // and a vote showing a higher committed slot
      val ballotNumber = BallotNumber(5, 2)
      val higherCommittedSlot = Identifier(3, ballotNumber, 99L)
      val higherCommittedProgress = Progress.highestPromisedHighestCommitted.set(initialDataWithTimeoutAndPrepareResponses.progress, (ballotNumber, higherCommittedSlot))
      val vote = PrepareNack(higherCommittedSlot, 3, higherCommittedProgress, 0, initialDataWithTimeoutAndPrepareResponses.leaderHeartbeat)
      // when it sees a higher committed slot index in a response
      handler.handleLowPrepareResponse(new TestIO(new UndefinedJournal){
        override def send(msg: PaxosMessage): Unit = sentMsg(msg)
        override def randomTimeout: Long = 999L
        override def respond(results: Option[Map[Identifier, Any]]): Unit = {}
      }, PaxosAgent(0, Follower, initialDataWithTimeoutAndPrepareResponses, initialQuorumStrategy), vote) match {
        case PaxosAgent(0, Follower, data, _) =>
          // it clears its prepares and sets a new timeout
          data.prepareResponses.size shouldBe 0
          data.timeout shouldBe 999L
        case x => fail(x.toString)
      }
      sentMsg() match {
        case RetransmitRequest(0, 3, logIndex) if logIndex == 0 => // good
        case x => fail(x.toString)
      }
    }
    "ignores a response that it is not waiting on" in {
      // given a handler
      val handler = new TestFollowerHandler
      // and a prepare response it is not waiting on
      val ballotNumber = BallotNumber(5, 2)
      val higherCommittedSlot = Identifier(3, ballotNumber, 0)
      val higherCommittedProgress = Progress.highestPromisedHighestCommitted.set(initialDataWithTimeoutAndPrepareResponses.progress, (ballotNumber, higherCommittedSlot))
      val vote = PrepareNack(higherCommittedSlot, 3, higherCommittedProgress, 0, initialDataWithTimeoutAndPrepareResponses.leaderHeartbeat)
      // when it sees that response it ignores it
      handler.handleLowPrepareResponse(new TestIO(new UndefinedJournal), PaxosAgent(0, Follower, initialDataWithTimeoutAndPrepareResponses, initialQuorumStrategy), vote) match {
        case PaxosAgent(_, Follower, data, _) =>
          data shouldBe initialDataWithTimeoutAndPrepareResponses
        case x => fail(x.toString)
      }
    }
    "collects responses when it does not have a majority response" in {
      val otherNodeUniqueId = 3
      // given a handler
      val handler = new TestFollowerHandler
      // and a prepare response it is waiting on
      val vote = PrepareNack(minPrepare.id, otherNodeUniqueId, initialDataWithTimeoutAndPrepareResponses.progress, 0, initialDataWithTimeoutAndPrepareResponses.leaderHeartbeat)
      // when it sees that response and does not have a majority as cluster size is 5
      handler.handleLowPrepareResponse(new TestIO(new UndefinedJournal){
      }, PaxosAgent(0, Follower, initialDataWithTimeoutAndPrepareResponses, initialQuorumStrategy5), vote) match {
        case PaxosAgent(_, Follower, data, _) => // it stays as follower
          data.prepareResponses.getOrElse(minPrepare.id, Map.empty).get(otherNodeUniqueId) match {
              case Some(`vote`) => // and has recorded the other nodes vote
              case x => fail(x.toString)
            }
        case x => fail(x.toString)
      }
    }
    "knows to failover when there are no other larger leader heartbeats" in {
      val nack1 = PrepareNack(minPrepare.id, 0, initialData.progress, 0, 999)
      val nack2 = PrepareNack(minPrepare.id, 0, initialData.progress, 0, 999)
      FollowerHandler.computeFailover(NoopPaxosLogging, 0, initialData.copy(leaderHeartbeat = 1000), Map(1 -> nack1, 2 -> nack2) , initialQuorumStrategy) match {
        case FailoverResult(true, 1000) => // good
        case x => fail(x.toString)
      }
    }
    "knows not to failover when there is sufficient evidence of other leaders heartbeat" in {
      val nack1 = PrepareNack(minPrepare.id, 0, initialData.progress, 0, 998)
      val nack2 = PrepareNack(minPrepare.id, 1, initialData.progress, 0, 999)
      FollowerHandler.computeFailover(NoopPaxosLogging, 0, initialData, Map(1 -> nack1, 2 -> nack2), initialQuorumStrategy) match {
        case FailoverResult(false, 999) => // good
        case x => fail(x.toString)
      }
    }
    "chooses to failover when there is insufficient evidence of other leaders heartbeat" in {
      val nack1 = PrepareNack(minPrepare.id, 0, initialData.progress, 0, 998)
      val nack2 = PrepareNack(minPrepare.id, 0, initialData.progress, 0, 999)
      FollowerHandler.computeFailover(NoopPaxosLogging, 0, initialData.copy(leaderHeartbeat = 998), Map(1 -> nack1, 2 -> nack2), initialQuorumStrategy5 ) match {
        case FailoverResult(true, 999) => // good
        case x => fail(x.toString)
      }
    }
    "computes high prepares for higher slots when should failover" in {
      val vote = PrepareNack(minPrepare.id, 1, initialData.progress, 3, 0)
      val handler = new TestFollowerHandler
      val highPrepares = ArrayBuffer[Prepare]()
      val journal = new UndefinedJournal {
        override def saveProgress(progress: Progress): Unit = {}

        override def accepted(logIndex: Long): Option[Accept] = None
      }
      handler.handleLowPrepareResponse(new TestIO(journal){
        override def send(msg: PaxosMessage): Unit = msg match {
          case p: Prepare => highPrepares += p
          case _ =>
        }
        override def randomTimeout: Long = 12345L
      }, PaxosAgent(99, Follower, initialDataWithTimeoutAndPrepareResponses.copy(leaderHeartbeat = 999L), initialQuorumStrategy), vote) match {
        case PaxosAgent(99, Recoverer, data, _) =>
          // issue #13 we are ignoring the highest accepted in the responses so only send 1 high prepare here rather than 3
          highPrepares.size shouldBe 1
          data.timeout shouldBe 12345L
          data.epoch match { // our highest promised should match our epoch and be higher than ballot numbers seen in the responses
            case Some(number) if number > minPrepare.id.number && number == data.progress.highestPromised => // good
            case x => fail(x.toString)
          }
          data.prepareResponses.size shouldBe 1
          // our high prepare should match our own self ack
          highPrepares.headOption match {
            case Some(prepare) =>
              data.prepareResponses.getOrElse(prepare.id, Map.empty).get(99) match {
              case Some(r: PrepareAck) if r.requestId == prepare.id => // good
              case x => fail(x.toString)
            }
            case _ => fail // unreachable
          }
        case x => fail(x.toString)
      }
    }
    "creates a prepare one higher than committed when highest accepted equals highest committed" in {
      val minIdentifier = Identifier(0, BallotNumber(0, 0), 0L)
      FollowerHandler.recoverPrepares(0, minIdentifier.number, 0L, 0L) match {
        case Seq(prepare) if prepare.id.logIndex == 1 && prepare.id.number == BallotNumber(1, 0) => // good
        case x => fail(x.toString())
      }
    }
    "creates two prepares if the highest accepted is one higher than the highest committed" in {
      val minIdentifier = Identifier(0, BallotNumber(0, 0), 0L)
      val higherNumber = BallotNumber(1, 0)
      FollowerHandler.recoverPrepares(0, minIdentifier.number, 0L, 1L) match {
        case Seq(p1, p2) =>
          p1 match {
            case p if p.id.logIndex == 1 && p.id.number == higherNumber => // good
            case x => fail(x.toString)
          }
          p2 match {
            case p if p.id.logIndex == 2 && p.id.number == higherNumber => // good
            case x => fail(x.toString)
          }
        case x => fail(x.toString())
      }
    }
    "resets if it sees sufficient evidence of a leader via heartbeats in low prepare responses" in {
      // given two nacks that see leader heartbeats at 999
      val nack1 = PrepareNack(minPrepare.id, 1, initialData.progress, 0, 999)
      val nack2 = PrepareNack(minPrepare.id, 2, initialData.progress, 0, 999)
      val votes = Map(1 -> nack1, 2 -> nack2)
      // and a Follower agent which last saw a heartbeat at 888
      val data =
        timeoutPrepareResponsesLens.set(initialData, (Long.MinValue,
          TreeMap(minPrepare.id -> votes)))
      val agent = PaxosAgent(0, Follower, data.copy(leaderHeartbeat = 888), initialQuorumStrategy)
      // and io with some timeout
      val io = new UndefinedIO with SilentLogging{
        override def randomTimeout: Long = 12345L

        override def respond(results: Option[Map[Identifier, Any]]): Unit = {}
      }
      // when
      val handler = new Object with FollowerHandler
      val PaxosAgent(_, _, newData, _) = handler.handleMajorityResponse(io, agent, votes)
      // then
      newData.prepareResponses.isEmpty shouldBe true
      newData.leaderHeartbeat shouldBe 999
      newData.timeout shouldBe 12345L
    }
  }

}
