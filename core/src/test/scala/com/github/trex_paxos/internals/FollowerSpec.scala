package com.github.trex_paxos.internals

import akka.actor.ActorSystem
import akka.testkit.{DefaultTimeout, ImplicitSender, TestActorRef, TestKit}
import com.github.trex_paxos.library._
import com.typesafe.config.{Config, ConfigFactory}
import org.scalamock.scalatest.MockFactory
import org.scalatest._

import scala.collection.immutable.SortedMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.postfixOps

object FollowerSpec {
  val config = ConfigFactory.parseString("trex.leader-timeout-min=10\ntrex.leader-timeout-max=20\nakka.loglevel = \"DEBUG\"")
}

class FollowerSpec
  extends TestKit(ActorSystem("FollowerSpec", FollowerSpec.config))
  with DefaultTimeout with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfter with MockFactory with OptionValues {

  import AllStateSpec._
  import Ordering._
  import PaxosActor.Configuration

  override def afterAll() {
    shutdown()
  }

  "Follower" should {

    val timenow = 999L

    val minPrepare = Prepare(Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), Long.MinValue))

    "backdown from a low prepare on receiving a fresh heartbeat commit from the same leader and request a retransmit" in {
      // given an initalized journal
      val stubJournal: Journal = stub[Journal]
      (stubJournal.load _) when() returns (Journal.minBookwork)

      // given that we control the clock
      val timenow = 999L
      val fsm = TestActorRef(new TestPaxosActor(Configuration(config, 3), 0, self, stubJournal, ArrayBuffer.empty, None) {
        override def clock() = timenow
      })
      // and no uncommitted values in journal
      val bounds = JournalBounds(0L, 0L)
      (stubJournal.bounds _) when() returns (bounds)
      // when our node gets a timeout
      fsm ! CheckTimeout
      // it sends out a single low prepare
      expectMsg(100 millisecond, minPrepare)
      // then when it receives a fresh heartbeat commit with a higher committed slow
      val freshCommit = Commit(Identifier(1, BallotNumber(lowValue, lowValue), 10L))
      fsm ! freshCommit
      // it responds with a retransmit
      expectMsg(100 millisecond, RetransmitRequest(0, 1, initialData.progress.highestCommitted.logIndex))
      // and has set the new heartbeat value
      assert(fsm.underlyingActor.data.leaderHeartbeat == freshCommit.heartbeat)
      // and has cleared the low prepare tracking map
      assert(fsm.underlyingActor.data.prepareResponses.isEmpty)
      // and sets a fresh timeout
      assert(fsm.underlyingActor.data.timeout > 0 && fsm.underlyingActor.data.timeout - timenow < config.getLong(PaxosActor.leaderTimeoutMaxKey))
    }
    "backdown from a low prepare on receiving a low heartbeat commit from a new leader" in {

      // given a follower in a cluster size of three
      val fsm = followerNoResponsesInClusterOfSize(3)
      // then when it receives a fresh heartbeat commit (we bump the slot number to induce a retransmit)
      val freshCommit = Commit(Identifier(1, BallotNumber(lowValue + 1, lowValue), 10L), -10L)
      fsm ! freshCommit
      // it responds with a retransmit
      expectMsg(100 millisecond, RetransmitRequest(0, 1, initialData.progress.highestCommitted.logIndex))
      // and has set the new heartbeat value
      assert(fsm.underlyingActor.data.leaderHeartbeat == freshCommit.heartbeat)
      // and has cleared the low prepare tracking map
      assert(fsm.underlyingActor.data.prepareResponses.isEmpty)
    }

    def followerNoResponsesInClusterOfSize(numberOfNodes: Int, highestAccepted: Long = 0L, cfg: Config = AllStateSpec.config) = {
      val prepareSelfVotes = SortedMap.empty[Identifier, Map[Int, PrepareResponse]] ++
        Seq((minPrepare.id -> Map(0 -> PrepareAck(minPrepare.id, 0, initialData.progress, 0, 0, None))))

      val state = initialData.copy(clusterSize = numberOfNodes, epoch = Some(minPrepare.id.number), prepareResponses = prepareSelfVotes)
      val fsm = TestActorRef(new TestPaxosActor(Configuration(cfg, numberOfNodes), 0, self, new TestAcceptMapJournal, ArrayBuffer.empty, None) {
        override def highestAcceptedIndex = highestAccepted
      })
      fsm.underlyingActor.setAgent(Follower, state)
      fsm
    }

    "switch to recoverer if in a five node cluster it sees a majority response with no heartbeats" in {
      // given a follower in a cluster size of five
      val fsm = followerNoResponsesInClusterOfSize(5)
      // when it receives two responses with no new heartbeat
      fsm ! PrepareNack(minPrepare.id, 1, initialData.progress, initialData.progress.highestCommitted.logIndex, initialData.leaderHeartbeat)
      fsm ! PrepareNack(minPrepare.id, 2, initialData.progress, initialData.progress.highestCommitted.logIndex, initialData.leaderHeartbeat)
      // then it promotes to be a recoverer
      fsm.underlyingActor.role should be(Recoverer)
      // and issues a higher prepare to the next slot
      expectMsg(100 millisecond, Prepare(Identifier(0, BallotNumber(lowValue + 1, 0), 1)))
    }

    "default to aggressive failover and promote to recoverer if in a five node cluster it sees a majority response and only one has a fresh heartbeats" in {
      // given a follower in a cluster size of five
      val fsm = followerNoResponsesInClusterOfSize(5)
      // when it receives two responses with no new heartbeat
      fsm ! PrepareNack(minPrepare.id, 1, initialData.progress, initialData.progress.highestCommitted.logIndex, initialData.leaderHeartbeat)
      fsm ! PrepareNack(minPrepare.id, 2, initialData.progress, initialData.progress.highestCommitted.logIndex, initialData.leaderHeartbeat + 1)
      // then it promotes to be a recoverer
      fsm.underlyingActor.role should be(Recoverer)
      // and issues a higher prepare to the next slot
      expectMsg(100 millisecond, Prepare(Identifier(0, BallotNumber(lowValue + 1, 0), 1)))
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

    // TODO should check that it ignores commits less than or equal to last committed logIndex (both of them)
  }
}

