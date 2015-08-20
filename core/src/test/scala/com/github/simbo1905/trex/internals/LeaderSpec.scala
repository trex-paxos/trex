package com.github.simbo1905.trex.internals

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit._
import com.github.simbo1905.trex.JournalBounds
import org.scalamock.scalatest.MockFactory
import org.scalatest._

import scala.collection.SortedMap
import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.postfixOps

class LeaderSpec
  extends TestKit(ActorSystem("LeaderSpec", AllStateSpec.config))
  with DefaultTimeout with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfter with MockFactory with OptionValues with AllStateSpec with LeaderLikeSpec {

  import AllStateSpec._
  import Ordering._
  import PaxosActor._

  override def afterAll() {
    shutdown()
  }

  val epoch = BallotNumber(1, 1)

  val dataNewEpoch = PaxosData.epochLens.set(initialData, Some(epoch))
  val initailLeaderData = PaxosData.leaderLens.set(dataNewEpoch, (SortedMap.empty[Identifier, Option[Map[Int, PrepareResponse]]], freshAcceptResponses, Map.empty))

  val expectedString2 = "Paxos"
  val expectedBytes2 = expectedString2.getBytes

  val expectedString3 = "Lamport"
  val expectedBytes3 = expectedString2.getBytes

  "Leader" should {
    "handles retransmission responses" in {
      retransmitRequestInvokesHandler(Leader)
    }
    "handles retransmission request" in {
      retransmitResponseInvokesHandler(Leader)
    }
    "nack a lower counter prepare" in {
      nackLowerCounterPrepare(Leader)
    }
    "nack a lower number prepare" in {
      nackLowerNumberedPrepare(Leader)
    }
    "ack a repeated prepare" in {
      ackRepeatedPrepare(Leader)
    }
    "accept higher prepare" in {
      ackHigherPrepare(Leader)
    }
    "journals commits not adjacent to last commit in retransmit response" in {
      journalsButDoesNotCommitIfNotContiguousRetransmissionResponse(Leader)
    }
    "journals accept messages and sets higher promise" in {
      journalsAcceptMessagesAndSetsHigherPromise(Follower)
    }
    "nack an accept lower than its last promise" in {
      nackAcceptLowerThanPromise(Leader)
    }
    "nack an accept for a slot which is committed" in {
      nackAcceptAboveCommitWatermark(Leader)
    }
    "ack duplidated accept" in {
      ackDuplicatedAccept(Leader)
    }
    "journal accepted messages" in {
      ackAccept(Leader)
    }
    "increments promise with higher accept" in {
      ackHigherAcceptMakingPromise(Leader)
    }
    "ignore commit message for lower log index" in {
      ignoreCommitMessageLogIndexLessThanLastCommit(Leader)
    }
    "ignore commit message equal than last committed lower nodeIdentifier" in {
      ignoreCommitMessageSameSlotLowerNodeIdentifier(Leader)
    }
    "backdown to follower on a commit of same slot but with higher node number" in {
      backdownToFollowerOnCommitSameSlotHigherNodeIdentifier(Leader)
    }
    "backdown to follower and request retransmission on commit higher than last committed" in {
      backdownToFollowerAndRequestRetransmissionOnCommitHigherThanLastCommitted(Leader)
    }
    "backdown to follower and perform commit" in {
      backdownToFollowerAndCommitOnCommitHigherThanLastCommitted(Leader)
    }

    val minPrepare = Prepare(Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), Long.MinValue))

    "ignores a late prepare response" in {
      // given a leader
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None))
      fsm.setState(Leader, initailLeaderData)
      // when we sent it a late prepare nack
      fsm ! PrepareNack(minPrepare.id, 2, initialData.progress, initialData.progress.highestCommitted.logIndex, Long.MaxValue)
      // then it does nothing
      expectNoMsg(25 milliseconds)
      fsm.stateName should be(Leader)
      fsm.stateData should be(initailLeaderData)
    }

    "boardcast client value data" in {

      expectNoMsg(10 millisecond)

      // given a leader
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None))
      fsm.setState(Leader, initailLeaderData)
      // when we sent it three log entries 
      fsm ! ClientRequestCommandValue(0, expectedBytes)
      fsm ! ClientRequestCommandValue(0, expectedBytes2)
      fsm ! ClientRequestCommandValue(0, expectedBytes3)
      // then it broadcasts them
      val a1 = Accept(Identifier(0, epoch, 1L), ClientRequestCommandValue(0, expectedBytes))
      val a2 = Accept(Identifier(0, epoch, 2L), ClientRequestCommandValue(0, expectedBytes2))
      val a3 = Accept(Identifier(0, epoch, 3L), ClientRequestCommandValue(0, expectedBytes3))
      expectMsg(100 milliseconds, a1)
      expectMsg(100 milliseconds, a2)
      expectMsg(100 milliseconds, a3)
      // stays as leader
      assert(fsm.stateName == Leader)
      // and creates a slot to record responses
      assert(fsm.stateData.acceptResponses.size == 3)
      // and holds slots in order
      assert(fsm.stateData.acceptResponses.keys.head.logIndex == 1)
      assert(fsm.stateData.acceptResponses.keys.last.logIndex == 3)
      // and has journalled the values
      (stubJournal.accept _).verify(Seq(a1))
      (stubJournal.accept _).verify(Seq(a2))
      (stubJournal.accept _).verify(Seq(a3))

    }

    "commit when it receives a majority of accept acks" in {
      // given a leader who has boardcast slot 99 and self voted on it and committed 98
      val lastCommitted = Identifier(0, epoch, 98L)
      val id99 = Identifier(0, epoch, 99L)
      val a99 = Accept(id99, ClientRequestCommandValue(0, Array[Byte](1, 1)))
      val votes = TreeMap(id99 -> AcceptResponsesAndTimeout(0L, a99, Map(0 -> AcceptAck(id99, 0, initialData.progress))))
      val responses = PaxosData.acceptResponsesLens.set(initialData, votes)
      val committed = Progress.highestPromisedHighestCommitted.set(responses.progress, (lastCommitted.number, lastCommitted))
      // and a journal which records when save as invoked
      var saveTime = 0L
      stubJournal.save _ when * returns {
        saveTime = System.nanoTime()
        Unit
      }
      var sendTime = 0L
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None) {
        override def send(actor: ActorRef, msg: Any): Unit = {
          sendTime = System.nanoTime()
          super.send(actor, msg)
        }
      })
      fsm.setState(Leader, responses.copy(progress = committed))
      // and the accept in the store
      val accept = Accept(id99, ClientRequestCommandValue(0, expectedBytes))
      (stubJournal.accepted _) when (99L) returns Some(accept)

      // when it gets one accept giving it a majority
      fsm ! AcceptAck(id99, 1, initialData.progress)

      // it sends the commit
      expectMsgPF(100 millisecond) {
        case Commit(prepareId, _) => Unit
      }

      // and delivered that value
      assert(fsm.underlyingActor.delivered.head == ClientRequestCommandValue(0, expectedBytes))
      // and journal bookwork
      (stubJournal.save _).verify(fsm.stateData.progress)
      // and deletes the pending work
      assert(fsm.stateData.acceptResponses.size == 0)
      // and send happens after save
      assert(saveTime > 0L && sendTime > 0L && saveTime < sendTime)
    }

    // TODO what to do if it recieves nacks?
    "return to follower when it receives a majority of accept nacks" in {
      // given two clients who have sent a request a leader
      val client1 = TestProbe()
      val client1msg = Identifier(0, epoch, 96L)
      val client1cmd = new CommandValue() {
        val msgId = 1L

        override def bytes: Array[Byte] = Array()
      }
      val client2 = TestProbe()
      val client2msg = Identifier(0, epoch, 97L)
      val client2cmd = new CommandValue() {
        val msgId = 2L

        override def bytes: Array[Byte] = Array()
      }
      val clientResponses: Map[Identifier, (CommandValue, ActorRef)] = Map(client1msg ->(client1cmd, client1.ref), client2msg ->(client2cmd, client2.ref))

      // given a leader who has boardcast slot 99 and self voted on it and committed 98
      val lastCommitted = Identifier(0, epoch, 98L)
      val id99 = Identifier(0, epoch, 99L)
      val a99 = Accept(id99, ClientRequestCommandValue(0, Array[Byte](1, 1)))

      val votes = TreeMap(id99 -> AcceptResponsesAndTimeout(0L, a99, Map(0 -> AcceptAck(id99, 0, initialData.progress))))
      val data = PaxosData.acceptResponsesClientCommandsLens.set(initialData, (votes, clientResponses))
      val committed = Progress.highestPromisedHighestCommitted.set(data.progress, (lastCommitted.number, lastCommitted))
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None) {
        override def freshTimeout(interval: Long): Long = 1234L
      })
      fsm.setState(Leader, data.copy(progress = committed))

      // when it gets one accept giving it a majority
      fsm ! AcceptNack(id99, 1, initialData.progress)
      fsm ! AcceptNack(id99, 2, initialData.progress)

      // it does not reply
      expectNoMsg(25 millisecond)
      // and returns to follower
      assert(fsm.stateName == Follower)
      // and deletes pending leader work
      assert(fsm.stateData.acceptResponses.isEmpty)
      assert(fsm.stateData.epoch == None)
      assert(fsm.stateData.prepareResponses.isEmpty)
      assert(fsm.stateData.clientCommands.isEmpty)
      // and sends an exception to the clients
      client1.expectMsg(10 millisecond, NoLongerLeaderException(0, 1L))
      client2.expectMsg(10 millisecond, NoLongerLeaderException(0, 2L))
      // and sets a fresh timeout
      fsm.stateData.timeout shouldBe 1234L
    }

    "commit in order when responses arrive out of order" in {
      // given accepts 99 and 100 are in the store with 98 committed
      val lastCommitted = Identifier(0, epoch, 98L)
      val id99 = Identifier(0, epoch, 99L)
      val a99 = Accept(id99, ClientRequestCommandValue(0, expectedBytes))
      (stubJournal.accepted _) when (99L) returns Some(a99)

      val id100 = Identifier(0, epoch, 100L)
      val a100 = Accept(id100, ClientRequestCommandValue(0, expectedBytes))
      (stubJournal.accepted _) when (100L) returns Some(a100)

      val votes = TreeMap(id99 -> AcceptResponsesAndTimeout(0L, a99, Map(0 -> AcceptAck(id99, 0, initialData.progress)))) + (id100 -> AcceptResponsesAndTimeout(0L, a100, Map(0 -> AcceptAck(id100, 0, initialData.progress))))

      val responses = PaxosData.acceptResponsesLens.set(initialData, votes)

      val committed = Progress.highestPromisedHighestCommitted.set(responses.progress, (lastCommitted.number, lastCommitted))

      // and a journal which records when save as invoked
      var saveTime = 0L
      stubJournal.save _ when * returns {
        saveTime = System.nanoTime()
        Unit
      }

      // and an actor which records the send time
      var sendTime = 0L
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None) {
        override def send(actor: ActorRef, msg: Any): Unit = {
          sendTime = System.nanoTime()
          super.send(actor, msg)
        }
      })
      fsm.setState(Leader, responses.copy(progress = committed))

      // when it gets one accept giving it a majority on 100
      fsm ! AcceptAck(id100, 1, initialData.progress)

      // it does not yet send the commit
      expectNoMsg(25 millisecond)

      // then gets a majority response to 99
      fsm ! AcceptAck(id99, 1, initialData.progress)

      // and commits 100
      expectMsgPF(100 millisecond) {
        case Commit(id100, _) => Unit
      }

      // and send happens after save
      assert(saveTime > 0L && sendTime > 0L && saveTime < sendTime)

      // and delivered both values
      assert(fsm.underlyingActor.delivered.size == 2)
      // updated bookwork
      assert(fsm.stateData.progress.highestCommitted.logIndex == 100)
      // and journal bookwork
      (stubJournal.save _).verify(fsm.stateData.progress)
      // and deletes the pending work
      assert(fsm.stateData.acceptResponses.size == 0)
    }

    // TODO surely this should not happen?
    "request retransmission when it sees a higher committed watermark in a majority response" in {
      // given accepts 99 and 100 are in the store can have committed up to 97
      val lastCommitted = Identifier(0, epoch, 97L)
      val id99 = Identifier(0, epoch, 99L)
      val a99 = Accept(id99, ClientRequestCommandValue(0, expectedBytes))
      (stubJournal.accepted _) when (99L) returns Some(a99)

      val id100 = Identifier(0, epoch, 100L)
      val a100 = Accept(id100, ClientRequestCommandValue(0, expectedBytes))
      (stubJournal.accepted _) when (100L) returns Some(a100)

      val votes = TreeMap(id99 -> AcceptResponsesAndTimeout(0L, a99, Map(0 -> AcceptAck(id99, 0, initialData.progress)))) + (id100 -> AcceptResponsesAndTimeout(0L, a100, Map(0 -> AcceptAck(id100, 0, initialData.progress))))

      val responses = PaxosData.acceptResponsesLens.set(initialData, votes)

      // and we have committed up to 97
      val committed = Progress.highestPromisedHighestCommitted.set(responses.progress, (lastCommitted.number, lastCommitted))

      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None))
      fsm.setState(Leader, responses.copy(progress = committed))

      // when it gets one accept giving it a majority on 100
      fsm ! AcceptAck(id100, 1, initialData.progress)

      // it does not yet send the commit
      expectNoMsg(25 millisecond)

      // then gets a majority response to 99 with a higher commit watermark indicating another node is up to 98
      fsm ! AcceptAck(id99, 1, Progress(epoch, Identifier(0, epoch, 98L)))

      // it sends a retransmit request
      expectMsg(100 millisecond, RetransmitRequest(0, 1, 97L))

      // and becomes a follower
      fsm.stateName should be(Follower)
      assert(fsm.stateData.acceptResponses.isEmpty)
    }

    "rebroadcasts its commit with a fresh heartbeat when it gets a prompt from the scheduler" in {
      // given a leader
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None))
      fsm.setState(Leader, initialData)
      // when it gets a tick
      fsm ! PaxosActor.HeartBeat
      // then it issues a heartbeat commit
      var commit: Commit = null
      expectMsgPF(100 millisecond) {
        case c: Commit => commit = c
      }
      val hb1 = commit.heartbeat
      // when it gets another tick
      Thread.sleep(2L)
      fsm ! PaxosActor.HeartBeat
      // it issues a fresh heartbeat commit
      expectMsgPF(100 millisecond) {
        case c: Commit => commit = c
      }
      val hb2 = commit.heartbeat
      assert(hb2 > hb1)
    }

    "reissue same accept messages it gets a timeout and no challenge" in {
      resendsNoLeaderChallenges(Leader)
    }

    "reissue higher accept messages upon learning of another nodes higher promise in a nack" in {
      resendsHigherAcceptOnLearningOtherNodeHigherPromise(Leader)
    }

    "reissues higher accept message upon having made a higher promise itself by the timeout" in {
      resendsHigherAcceptOnHavingMadeAHigherPromiseAtTimeout(Leader)
    }

    "does backs down to be a follower when it makes a higher promise to another node" in {
      expectNoMsg(10 millisecond)

      // given low initial state committed up to 97
      val low = BallotNumber(5, 1)
      val initialData = PaxosData(Progress(low, minIdentifier), leaderHeartbeat2, timeout4, clusterSize5)
      val lastCommitted = Identifier(0, epoch, 97L)
      val committed = Progress.highestPromisedHighestCommitted.set(initialData.progress, (lastCommitted.number, lastCommitted))
      // and some other node high prepare for slot 98
      val high = BallotNumber(10, 1)
      val highIdentifier = Identifier(0, high, 98L)
      val highPrepare = Prepare(highIdentifier)
      // given no previous value for that slot
      (stubJournal.accepted _) when (98L) returns None
      (stubJournal.bounds _) when() returns (JournalBounds(0L, 97L))
      // the leader
      val timenow = 999L
      val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize5), 0, self, stubJournal, ArrayBuffer.empty, None) {
        override def clock() = timenow
      })
      fsm.setState(Leader, initialData.copy(epoch = Some(epoch), progress = committed))
      // when it gets the high prepare
      fsm ! highPrepare
      // then it responds with a ack
      expectMsgPF(250 millisecond) {
        case ack: PrepareAck =>
      }
      // and returns to be follower
      assert(fsm.stateName == Follower)
      // and clears data
      assert(fsm.stateData.acceptResponses.isEmpty)
      assert(fsm.stateData.prepareResponses.isEmpty)
      assert(fsm.stateData.epoch == None)
      // and sets a fresh timeout
      assert(fsm.stateData.timeout > 0 && fsm.stateData.timeout - timenow < config.getLong(PaxosActor.leaderTimeoutMaxKey))
    }
  }

  // TODO ensure that when we leave a given state we gc any state not needed for the state we are jumping to. suggests one long lens for all the optional stuff

  // TODO get the underlying actor ref and check its illegal argument states
}
