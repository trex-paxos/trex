package com.github.simbo1905.trex.internals

import org.scalatest._
import org.scalamock.scalatest.MockFactory
import com.typesafe.config.ConfigFactory
import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ DefaultTimeout, ImplicitSender, TestKit }
import akka.testkit.TestFSMRef
import scala.collection.SortedMap
import scala.collection.mutable.ArrayBuffer
import com.github.simbo1905.trex.{JournalBounds, Journal}
import com.typesafe.config.Config
import scala.compat.Platform
import scala.language.postfixOps
import scala.concurrent.duration._

object FollowerSpec {
  val config = ConfigFactory.parseString("trex.leader-timeout-min=10\ntrex.leader-timeout-max=20\nakka.loglevel = \"DEBUG\"")
}

class FollowerSpec extends TestKit(ActorSystem("FollowerSpec",
  FollowerSpec.config))
  with DefaultTimeout with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfter with MockFactory with OptionValues with AllStateSpec with FollowerLikeSpec {

  import AllStateSpec._
  import PaxosActor.Configuration
  import Ordering._

  override def afterAll() {
    shutdown()
  }

  "Follower" should {
    "handles retransmission responses" in {
      retransmitRequestInvokesHandler(Follower)
    }
    "handles retransmission request" in {
      retransmitResponseInvokesHandler(Follower)
    }
    "respond to client data by saying that it is not the leader" in {
      respondsToClientDataBySayingNotTheLeader(Follower)
    }
    "nack a lower counter prepare" in {
      nackLowerCounterPrepare(Follower)
    }
    "nack a lower numbered prepare" in {
      nackLowerNumberedPrepare(Follower)
    }
    "ack a repeated prepare" in {
      ackRepeatedPrepare(Follower)
    }
    "ack higher prepare" in {
      ackHigherPrepare(Follower)
    }
    "journals commits not adjacent to last commit in retransmit response" in {
      journalsButDoesNotCommitIfNotContiguousRetransmissionResponse(Follower)
    }
    "journals accept messages and sets higher promise" in {
      journalsAcceptMessagesAndSetsHigherPromise(Follower)
    }
    "bootstrap from retransmit response" in {
      // given some retransmitted committed values
      val v1 = ClientRequestCommandValue(0, Array[Byte]{0})
      val v2 = ClientRequestCommandValue(1, Array[Byte]{1})
      val v3 = ClientRequestCommandValue(2, Array[Byte]{2})
      val a1 =
        Accept(Identifier(1,BallotNumber(1, 1), 1L), v1)
      val a2 =
        Accept(Identifier(2,BallotNumber(2, 2), 2L), v2)
      val a3 =
        Accept(Identifier(3,BallotNumber(3, 3), 3L), v3)
      val retransmission = RetransmitResponse(1, 0, Seq(a1, a2, a3), Seq.empty)

      // and an empty node
      val fileJournal: FileJournal = AllStateSpec.tempFileJournal
      val delivered = ArrayBuffer[CommandValue]()
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, fileJournal, delivered, None))

      // when the retransmission is received
      fsm ! retransmission

      // it sends no messages
      expectNoMsg(25 milliseconds)
      // stays in state
      assert(fsm.stateName == Follower)
      // updates its commit index
      assert(fsm.stateData.progress.highestCommitted == a3.id)

      // delivered the committed values
      delivered.size should be (3)
      delivered(0) should be (v1)
      delivered(1) should be (v2)
      delivered(2) should be (v3)

      // journaled the values so that it can retransmit itself
      fileJournal.bounds should be (JournalBounds(1,3))
      fileJournal.accepted(1) match {
        case Some(a) if a.id == a1.id => // good
      }
      fileJournal.accepted(2) match {
        case Some(a) if a.id == a2.id => // good
      }
      fileJournal.accepted(3) match {
        case Some(a) if a.id == a3.id => // good
      }

      // and journal the new progress
      fileJournal.load() match {
        case Progress(_, a3.id) => // good
        case p => fail(s"got $p not ${Progress(a3.id.number, a3.id)}")
      }
    }
    "nack an accept lower than its last promise" in {
      nackAcceptLowerThanPromise(Follower)
    }
    "nack an accept for a slot which is committed" in {
      nackAcceptAboveCommitWatermark(Follower)
    }
    "ack duplidated accept" in {
      ackDuplicatedAccept(Follower)
    }
    "journals accepted message" in {
      ackAccept(Follower)
    }
    "increments promise with higher accept" in {
      ackHigherAcceptMakingPromise(Follower)
    }
    "not switch to recoverer if it does not timeout" in {
      // when our node has a high timeout
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None))
      fsm.setState(Follower, initialData.copy(timeout = System.currentTimeMillis + minute))
      // and we sent it a timeout check message
      fsm ! PaxosActor.CheckTimeout
      // it sends no messages
      expectNoMsg(25 millisecond)
      // and does not change state
      assert(fsm.stateName == Follower)
    }
    val timenow = 999L
    "update its timeout when it sees a commit" in {
      // given an initalized journal
      (stubJournal.load _) when () returns (Journal.minBookwork)
      // given we control the clock
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None) {
        override def clock() = timenow
      })
      fsm ! Commit(Identifier(0, BallotNumber(lowValue, lowValue), 0L), 9999L)
      // it sends no messages
      expectNoMsg(25 millisecond)
      // and sets a fresh timeout
      assert(fsm.stateData.timeout > 0 && fsm.stateData.timeout - timenow < config.getLong(PaxosActor.leaderTimeoutMaxKey))
      // and updates its heartbeat
      fsm.stateData.leaderHeartbeat should be(9999L)
    }
    "update its heartbeat when it sees a commit" in {
      // given we control the clock
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None) {
        override def clock() = timenow
      })
      fsm.setState(Follower, initialData.copy(leaderHeartbeat = 88L))
      fsm ! Commit(Identifier(0, BallotNumber(lowValue, lowValue), 0), 99L)
      // it sends no messages
      expectNoMsg(25 millisecond)
      // and updates the heartbeat
      assert(fsm.stateData.leaderHeartbeat == 99L)
    }
    "commit next slot if same number as previous commit" in {
      // given an initialized journal
      (stubJournal.load _) when () returns (Journal.minBookwork)

      // given slot 1 has been accepted under the same number as previously committed slot 0 shown in initialData
      val identifier = Identifier(0, BallotNumber(lowValue, lowValue), 1L)
      val accepted = Accept(identifier, ClientRequestCommandValue(0, expectedBytes))
      (stubJournal.accepted _) when (1L) returns Some(accepted)

      // when we commit that value
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None) {
        override def clock() = timenow
      })
      fsm ! Commit(identifier)

      // then it sends no messages
      expectNoMsg(25 millisecond)
      // and delivered that value
      assert(fsm.underlyingActor.delivered.head == ClientRequestCommandValue(0, expectedBytes))
      // and journal bookwork
      (stubJournal.save _).verify(fsm.stateData.progress)
      // and sets a fresh timeout
      assert(fsm.stateData.timeout > 0 && fsm.stateData.timeout - timenow < config.getLong(PaxosActor.leaderTimeoutMaxKey))
    }
    "commit next slot on a different number as previous commit" in {
      // given an initialized journal
      (stubJournal.load _) when () returns (Journal.minBookwork)

      // given slot 1 has been accepted under a different number as previously committed slot 0 shown in initialData
      val identifier = Identifier(0, BallotNumber(0, 0), 1L)
      val accepted = Accept(identifier, ClientRequestCommandValue(0, expectedBytes))
      (stubJournal.accepted _) when (1L) returns Some(accepted)

      // when we commit that value
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None) {
        override def clock() = timenow
      })
      fsm ! Commit(identifier)

      // then it sends no messages
      expectNoMsg(25 millisecond)
      // and delivered that value
      assert(fsm.underlyingActor.delivered.head == ClientRequestCommandValue(0, expectedBytes))
      // and journal bookwork
      (stubJournal.save _).verify(fsm.stateData.progress)
      // and sets a fresh timeout
      assert(fsm.stateData.timeout > 0 && fsm.stateData.timeout - timenow < config.getLong(PaxosActor.leaderTimeoutMaxKey))
    }
    "request retranmission if commit of next in slot does not match value in slot" in {
      // given an initalized journal
      (stubJournal.load _) when () returns (Journal.minBookwork)

      // given slot 1 has been accepted under the same number as previously committed slot 0 shown in initialData
      val identifierMin = Identifier(0, BallotNumber(lowValue, lowValue), 1L)
      val accepted = Accept(identifierMin, ClientRequestCommandValue(0, expectedBytes))
      (stubJournal.accepted _) when (1L) returns Some(accepted)

      val otherNodeId = 1
      // when we commit a value into that slot using a different number
      val identifier99 = Identifier(otherNodeId, BallotNumber(0, 99), 1L)
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None) {
        override def clock() = timenow
      })
      fsm ! Commit(identifier99)
      // then it sends a retransmit request
      expectMsg(100 millisecond, RetransmitRequest(0, otherNodeId, 0L))
      // and did not commit
      assert(fsm.underlyingActor.delivered.isEmpty)
      // and sets a fresh timeout
      assert(fsm.stateData.timeout > 0 && fsm.stateData.timeout - timenow < config.getLong(PaxosActor.leaderTimeoutMaxKey))
    }
    "commit if three slots if previous accepts are from stable leader" in {
      // given an initalized journal
      (stubJournal.load _) when () returns (Journal.minBookwork)

      // given slots 1 thru 3 have been accepted under the same number as previously committed slot 0 shown in initialData

      val id1 = Identifier(0, BallotNumber(lowValue, lowValue), 1L)
      (stubJournal.accepted _) when (1L) returns Some(Accept(id1, ClientRequestCommandValue(0, expectedBytes)))

      val id2 = Identifier(0, BallotNumber(lowValue, lowValue), 2L)
      (stubJournal.accepted _) when (2L) returns Some(Accept(id2, ClientRequestCommandValue(0, expectedBytes)))

      val id3 = Identifier(0, BallotNumber(lowValue, lowValue), 3L)
      (stubJournal.accepted _) when (3L) returns Some(Accept(id3, ClientRequestCommandValue(0, expectedBytes)))

      // when we commit slot 3
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None) {
        override def clock() = timenow
      })
      fsm ! Commit(id3)

      // then it sends no messages
      expectNoMsg(25 millisecond)
      // and delivered those values
      assert(fsm.underlyingActor.delivered.size == 3)
      // and updates its latest commit
      assert(fsm.stateData.progress.highestCommitted == id3)
      // and journals progress
      (stubJournal.save _).verify(fsm.stateData.progress)
      // and sets a fresh timeout
      assert(fsm.stateData.timeout > 0 && fsm.stateData.timeout - timenow < config.getLong(PaxosActor.leaderTimeoutMaxKey))
    }
    "request retransmission if it sees a gap in commit sequence" in {
      // given an initalized journal
      (stubJournal.load _) when () returns (Journal.minBookwork)

      // given slots 1 thru 3 have been accepted under the same number as previously committed slot 0 shown in initialData
      val otherNodeId = 1

      val id1 = Identifier(otherNodeId, BallotNumber(lowValue, 99), 1L)
      (stubJournal.accepted _) when (1L) returns Some(Accept(id1, ClientRequestCommandValue(0, expectedBytes)))

      val id2 = Identifier(otherNodeId, BallotNumber(lowValue, 99), 2L)
      (stubJournal.accepted _) when (2L) returns None // gap

      val id3 = Identifier(otherNodeId, BallotNumber(lowValue, 99), 3L)
      (stubJournal.accepted _) when (3L) returns Some(Accept(id3, ClientRequestCommandValue(0, expectedBytes)))

      // when we commit slot 3
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None))
      fsm ! Commit(id3)

      // it sends retransmission
      expectMsg(100 milliseconds, RetransmitRequest(0, otherNodeId, 1L))
      // commits up to the gap
      assert(fsm.underlyingActor.delivered.size == 1)
      // and updates its latest commit
      assert(fsm.stateData.progress.highestCommitted == id1)
      // and journals progress
      (stubJournal.save _).verify(fsm.stateData.progress)
    }
    "request retransmission if it sees value under a different number in a commit sequence" in {
      // given an journal with a promise to node1 and committed up to last from node2
      val node1 = 1
      val node2 = 2

      (stubJournal.load _) when () returns (Progress(BallotNumber(99, node1), Identifier(node2, BallotNumber(98, node2),0L)))

      // given slots 1 and 3 match the promise but slot 2 has old value from failed leader.

      val id1 = Identifier(node1, BallotNumber(99, node1), 1L)
      (stubJournal.accepted _) when (1L) returns Some(Accept(id1, ClientRequestCommandValue(0, expectedBytes)))

      val id2other = Identifier(node2, BallotNumber(98, node2), 2L)
      (stubJournal.accepted _) when (2L) returns Some(Accept(id2other, ClientRequestCommandValue(0, expectedBytes)))

      val id3 = Identifier(node1, BallotNumber(99, node1), 3L)
      (stubJournal.accepted _) when (3L) returns Some(Accept(id3, ClientRequestCommandValue(0, expectedBytes)))

      // when we commit slot 3
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None))
      fsm ! Commit(id3)

      // it sends retransmission
      expectMsg(100 milliseconds, RetransmitRequest(0, node1, 1L))
      // commits up to the gap
      assert(fsm.underlyingActor.delivered.size == 1)
      // and updates its latest commit
      assert(fsm.stateData.progress.highestCommitted == id1)
      // and journals progress
      (stubJournal.save _).verify(fsm.stateData.progress)
    }
    "request retransmission and does not commit if it has wrong values in slots" in {
      // given slots 1 thru 3 have been accepted under a different as previously committed slot 0 shown in initialData

      val otherNodeId = 1

      val id1other = Identifier(otherNodeId, BallotNumber(0, 0), 1L)
      (stubJournal.accepted _) when (1L) returns Some(Accept(id1other, ClientRequestCommandValue(0, expectedBytes)))

      val id2other = Identifier(otherNodeId, BallotNumber(0, 0), 2L)
      (stubJournal.accepted _) when (2L) returns Some(Accept(id2other, ClientRequestCommandValue(0, expectedBytes)))

      val id3other = Identifier(otherNodeId, BallotNumber(0, 0), 3L)
      (stubJournal.accepted _) when (3L) returns Some(Accept(id3other, ClientRequestCommandValue(0, expectedBytes)))

      // when we commit slot 3
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None))
      fsm.setState(Follower, initialData)

      fsm ! Commit(Identifier(otherNodeId, BallotNumber(lowValue, lowValue), 3L))

      // it sends retransmission
      expectMsg(100 milliseconds, RetransmitRequest(0, otherNodeId, fsm.stateData.progress.highestCommitted.logIndex))
      // commits up to the gap
      assert(fsm.underlyingActor.delivered.size == 0)
      // and updates its latest commit
      assert(fsm.stateData.progress.highestCommitted == initialData.progress.highestCommitted)
    }

    val minPrepare = Prepare(Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), Long.MinValue))

    "times-out and issues a single low prepare" in {
      // given that we control the clock
      val timenow = 999L
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None) {
        override def clock() = timenow
      })
      // and no uncommitted values in journal
      val bounds = JournalBounds(0L, 0L)
      (stubJournal.bounds _) when () returns (bounds)
      // when our node gets a timeout
      fsm ! PaxosActor.CheckTimeout
      // it sends out a single low prepare
      expectMsg(100 millisecond, minPrepare)
      // and stays as follower
      assert(fsm.stateName == Follower)
      // and sets a fresh timeout
      assert(fsm.stateData.timeout > 0 && fsm.stateData.timeout - timenow < config.getLong(PaxosActor.leaderTimeoutMaxKey))
      // and is tracking reponses to its low prepare
      assert(fsm.stateData.prepareResponses != None)
      // and has responded itself
      assert(fsm.stateData.prepareResponses.get(minPrepare.id) != None)
      assert(fsm.stateData.prepareResponses.get(minPrepare.id).get.size == 1)
    }
    "re-issues a low prepare on subsequent time-outs when it has not recieved any responses" in {
      // given that we control the clock
      var timenow = 999L
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None) {
        override def clock() = timenow
      })
      // and no uncommitted values in journal
      val bounds = JournalBounds(0L, 0L)
      (stubJournal.bounds _) when () returns (bounds)
      // when our node gets a timeout
      fsm ! PaxosActor.CheckTimeout
      // it sends out a single low prepare
      expectMsg(100 millisecond, minPrepare)
      // which repeats on subsequent time outs
      timenow = timenow + fsm.stateData.timeout + 1
      fsm ! PaxosActor.CheckTimeout
      expectMsg(100 millisecond, minPrepare)
      timenow = timenow + fsm.stateData.timeout + 1
      fsm ! PaxosActor.CheckTimeout
      expectMsg(100 millisecond, minPrepare)
      timenow = timenow + fsm.stateData.timeout + 1
      fsm ! PaxosActor.CheckTimeout
      expectMsg(100 millisecond, minPrepare)
      // and stays as follower
      assert(fsm.stateName == Follower)
      // and is tracking responses to its low prepare
      assert(fsm.stateData.prepareResponses != None)
      // and has responded itself
      assert(fsm.stateData.prepareResponses.get(minPrepare.id) != None)
      assert(fsm.stateData.prepareResponses.get(minPrepare.id).get.size == 1)
    }
    "backdown from a low prepare on receiving a fresh heartbeat commit from the same leader and request a retransmit" in {
      // given an initalized journal
      (stubJournal.load _) when () returns (Journal.minBookwork)

      // given that we control the clock
      val timenow = 999L
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None) {
        override def clock() = timenow
      })
      // and no uncommitted values in journal
      val bounds = JournalBounds(0L, 0L)
      (stubJournal.bounds _) when () returns (bounds)
      // when our node gets a timeout
      fsm ! PaxosActor.CheckTimeout
      // it sends out a single low prepare
      expectMsg(100 millisecond, minPrepare)
      // then when it receives a fresh heartbeat commit with a higher committed slow
      val freshCommit = Commit(Identifier(1, BallotNumber(lowValue, lowValue), 10L))
      fsm ! freshCommit
      // it responds with a retransmit
      expectMsg(100 millisecond, RetransmitRequest(0, 1, initialData.progress.highestCommitted.logIndex))
      // and has set the new heartbeat value
      assert(fsm.stateData.leaderHeartbeat == freshCommit.heartbeat)
      // and has cleared the low prepare tracking map
      assert(fsm.stateData.prepareResponses.isEmpty)
      // and sets a fresh timeout
      assert(fsm.stateData.timeout > 0 && fsm.stateData.timeout - timenow < config.getLong(PaxosActor.leaderTimeoutMaxKey))
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
      assert(fsm.stateData.leaderHeartbeat == freshCommit.heartbeat)
      // and has cleared the low prepare tracking map
      assert(fsm.stateData.prepareResponses.isEmpty)
    }
    "backdown from a low prepare if other follower has a higher heartbeat" in {
      // given a follower in a cluster size of three
      val fsm = followerNoResponsesInClusterOfSize(3)

      // when it hears back that the other follower has seen a higher heartbeat
      fsm ! PrepareNack(minPrepare.id, 2, initialData.progress, initialData.progress.highestCommitted.logIndex, Long.MaxValue)

      // and backs down
      fsm.stateName should be(Follower)
      fsm.stateData.prepareResponses.size should be(0)
      // and updates its the known leader heartbeat so that if the leader dies it can take over
      fsm.stateData.leaderHeartbeat should be(Long.MaxValue)
    }
    "backdown from a low prepare if other follower has committed a higher slot" in {
      // given a follower in a cluster size of three
      val fsm = followerNoResponsesInClusterOfSize(3)

      val otherFollowerId = 2

      // when it hears back that the other follower has committed a higher slot
      fsm ! PrepareNack(minPrepare.id, otherFollowerId, Progress.highestCommittedLens.set(initialData.progress, initialData.progress.highestCommitted.copy(logIndex = 99L)), initialData.progress.highestCommitted.logIndex + 1, Long.MaxValue)

      // it responds with a retransmit
      expectMsg(100 millisecond, RetransmitRequest(0, otherFollowerId, initialData.progress.highestCommitted.logIndex))

      // and backs down
      fsm.stateName should be(Follower)
      fsm.stateData.prepareResponses.size should be(0)
    }

    def followerNoResponsesInClusterOfSize(numberOfNodes: Int, highestAccepted: Long = 0L, cfg: Config = AllStateSpec.config) = {
      val prepareSelfVotes = SortedMap.empty[Identifier, Option[Map[Int, PrepareResponse]]] ++
        Seq((minPrepare.id -> Some(Map(0 -> PrepareAck(minPrepare.id, 0, initialData.progress, 0, 0, None)))))

      val state = initialData.copy(clusterSize = numberOfNodes, epoch = Some(minPrepare.id.number), prepareResponses = prepareSelfVotes)
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(cfg, numberOfNodes), 0, self, new TestAcceptMapJournal, ArrayBuffer.empty, None) {
        override def highestAcceptedIndex = highestAccepted
      })
      fsm.setState(Follower, state)
      fsm
    }

    "switch to recoverer if in a five node cluster it sees a majority response with no heartbeats" in {
      // given a follower in a cluster size of five
      val fsm = followerNoResponsesInClusterOfSize(5)
      // when it receives two responses with no new heartbeat
      fsm ! PrepareNack(minPrepare.id, 1, initialData.progress, initialData.progress.highestCommitted.logIndex, initialData.leaderHeartbeat)
      fsm ! PrepareNack(minPrepare.id, 2, initialData.progress, initialData.progress.highestCommitted.logIndex, initialData.leaderHeartbeat)
      // then it promotes to be a recoverer
      fsm.stateName should be(Recoverer)
      // and issues a higher prepare to the next slot
      expectMsg(100 millisecond, Prepare(Identifier(0, BallotNumber(lowValue + 1, 0), 1)))
      expectNoMsg(10 millisecond)
    }

    "default to aggressive failover and promote to recoverer if in a five node cluster it sees a majority response and only one has a fresh heartbeats" in {
      // given a follower in a cluster size of five
      val fsm = followerNoResponsesInClusterOfSize(5)
      // when it receives two responses with no new heartbeat
      fsm ! PrepareNack(minPrepare.id, 1, initialData.progress, initialData.progress.highestCommitted.logIndex, initialData.leaderHeartbeat)
      fsm ! PrepareNack(minPrepare.id, 2, initialData.progress, initialData.progress.highestCommitted.logIndex, initialData.leaderHeartbeat + 1)
      // then it promotes to be a recoverer
      fsm.stateName should be(Recoverer)
      // and issues a higher prepare to the next slot
      expectMsg(100 millisecond, Prepare(Identifier(0, BallotNumber(lowValue + 1, 0), 1)))
      expectNoMsg(10 millisecond)
    }

    "switch to recoverer and issue multiple prepare messages if there are slots to recover and no leader" in {

      // given that we control the clock
      val timenow = 999L
      var sendTime = 0L
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None) {
        override def clock() = timenow
        override def send(actor: ActorRef, msg: Any): Unit = {
          sendTime = System.nanoTime()
          actor ! msg
        }
      })
      fsm.setState(Follower, initialData)

      // and three uncommitted values in journal
      val bounds = JournalBounds(0L, 3L)
      (stubJournal.bounds _) when () returns (bounds)

      val id1 = Identifier(0, BallotNumber(lowValue + 1, 0), 1)
      (stubJournal.accepted _) when (1L) returns Some(Accept(id1, ClientRequestCommandValue(0, expectedBytes)))

      val id2 = Identifier(0, BallotNumber(lowValue + 1, 0), 2)
      (stubJournal.accepted _) when (2L) returns Some(Accept(id2, ClientRequestCommandValue(0, expectedBytes)))

      val id3 = Identifier(0, BallotNumber(lowValue + 1, 0), 3)
      (stubJournal.accepted _) when (3L) returns Some(Accept(id3, ClientRequestCommandValue(0, expectedBytes)))

      // and a journal which records the save time
      var saveTime = 0L
      (stubJournal.save _) when(*) returns {
        saveTime = System.nanoTime()
        Unit
      }

      val id4 = Identifier(0, BallotNumber(lowValue + 1, 0), 4)

      // when our node gets a timeout
      fsm ! PaxosActor.CheckTimeout

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
      assert(fsm.stateName == Recoverer)
      // and sets a fresh timeout
      assert(fsm.stateData.timeout > 0 && fsm.stateData.timeout - timenow < config.getLong(PaxosActor.leaderTimeoutMaxKey))
      // and make a promise to self
      assert(fsm.stateData.progress.highestPromised == prepare1.id.number)
      // and votes for its own prepares
      assert(!fsm.stateData.prepareResponses.isEmpty)
      val prapareIds = fsm.stateData.prepareResponses map {
        case (id, Some(map)) if map.keys.head == 0 && map.values.head.requestId == id =>
          id
        case _ =>
          fail()
      }
      assert(true == prapareIds.toSet.contains(prepare1.id))
      assert(true == prapareIds.toSet.contains(prepare2.id))
      assert(true == prapareIds.toSet.contains(prepare3.id))

      // and has no accepts
      assert(fsm.stateData.acceptResponses.isEmpty)

      // and it sent out the messages only after having journalled its own promise
      assert(saveTime != 0 && sendTime != 0 && saveTime < sendTime)

      // FIXME should use a test file journal and confirm that saves and sends are called in the correct order
    }

    // TODO should check that it ignores commits less than or equal to last committed logIndex (both of them)
  }

  // TODO ensure that when we leave a given state we gc any state not needed for the state we are jumping to. suggests one long lens for all the optional stuff

  // TODO get the underlying actor ref and check its illegal argument states
}

