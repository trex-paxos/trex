package com.github.simbo1905.trex.internals

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit._
import com.github.simbo1905.trex.library._
import org.scalamock.scalatest.MockFactory
import org.scalatest._

import scala.collection.immutable.{TreeMap, SortedMap}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.postfixOps

class LeaderSpec
  extends TestKit(ActorSystem("LeaderSpec", AllStateSpec.config))
  with DefaultTimeout with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfter with MockFactory with OptionValues with AllStateSpec with LeaderLikeSpec with PaxosLenses[ActorRef] {

  import AllStateSpec._
  import Ordering._
  import PaxosActor._

  override def afterAll() {
    shutdown()
  }

  val epoch = BallotNumber(1, 1)

  val dataNewEpoch = epochLens.set(initialData, Some(epoch))
  val initailLeaderData = leaderLens.set(dataNewEpoch, (SortedMap.empty[Identifier, Map[Int, PrepareResponse]], freshAcceptResponses, Map.empty))

  val expectedString2 = "Paxos"
  val expectedBytes2 = expectedString2.getBytes

  val expectedString3 = "Lamport"
  val expectedBytes3 = expectedString2.getBytes

  "Leader" should {
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
      journalsAcceptMessagesAndSetsHigherPromise(Leader)
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
      val stubJournal: Journal = stub[Journal]
      // given a leader
      val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None))
      fsm.underlyingActor.setAgent(Leader, initailLeaderData)
      // when we sent it a late prepare nack
      fsm ! PrepareNack(minPrepare.id, 2, initialData.progress, initialData.progress.highestCommitted.logIndex, Long.MaxValue)
      // then it does nothing
      expectNoMsg(25 milliseconds)
      fsm.underlyingActor.role should be(Leader)
      fsm.underlyingActor.data should be(initailLeaderData)
    }

    "boardcast client value data" in {
      expectNoMsg(10 millisecond)
      val stubJournal: Journal = stub[Journal]
      // given a leader
      val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None))
      fsm.underlyingActor.setAgent(Leader, initailLeaderData)
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
      assert(fsm.underlyingActor.role == Leader)
      // and creates a slot to record responses
      assert(fsm.underlyingActor.data.acceptResponses.size == 3)
      // and holds slots in order
      fsm.underlyingActor.data.acceptResponses.keys.headOption match {
        case Some(id) => assert(id.logIndex == 1)
        case x => fail(x.toString)
      }
      fsm.underlyingActor.data.acceptResponses.keys.lastOption match {
        case Some(id) => assert(id.logIndex == 3)
        case x => fail(x.toString)
      }
      // and has journalled the values
      (stubJournal.accept _).verify(Seq(a1))
      (stubJournal.accept _).verify(Seq(a2))
      (stubJournal.accept _).verify(Seq(a3))

    }

    "commit when it receives a majority of accept acks" in {
      // given a journal which records the save time and the progress
      var saveTime = 0L
      var savedProgress: Option[Progress] = None
      val stubJournal: Journal = stub[Journal]
      val delegatingJournal = new DelegatingJournal(stubJournal) {
        override def save(progress: Progress): Unit = {
          saveTime = System.nanoTime()
          savedProgress = Option(progress)
        }
      }
      // and a leader who has boardcast slot 99 and self voted on it and committed 98
      val lastCommitted = Identifier(0, epoch, 98L)
      val id99 = Identifier(0, epoch, 99L)
      val a99 = Accept(id99, ClientRequestCommandValue(0, Array[Byte](1, 1)))
      val votes = TreeMap(id99 -> AcceptResponsesAndTimeout(0L, a99, Map(0 -> AcceptAck(id99, 0, initialData.progress))))
      val responses = acceptResponsesLens.set(initialData, votes)
      val committed = Progress.highestPromisedHighestCommitted.set(responses.progress, (lastCommitted.number, lastCommitted))
      var sendTime = 0L
      val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, delegatingJournal, ArrayBuffer.empty, None) {
        override def broadcast(msg: PaxosMessage): Unit = {
          sendTime = System.nanoTime()
          super.broadcast(msg)}
      })
      fsm.underlyingActor.setAgent(Leader, responses.copy(progress = committed))
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
      fsm.underlyingActor.delivered.headOption match {
        case Some(c) => assert(c == ClientRequestCommandValue(0, expectedBytes))
        case x => fail(x.toString)
      }
      // and journal bookwork
      savedProgress match {
        case Some(p) if p == fsm.underlyingActor.data.progress => // good
        case x => fail(x.toString)
      }
      // and deletes the pending work
      assert(fsm.underlyingActor.data.acceptResponses.size == 0)
      // and send happens after save
      assert(saveTime > 0L && sendTime > 0L && saveTime < sendTime)
    }

    // TODO what to do if it recieves nacks?
    "return to follower when it receives a majority of accept nacks" in {
      val stubJournal: Journal = stub[Journal]
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
      val data = acceptResponsesClientCommandsLens.set(initialData, (votes, clientResponses))
      val committed = Progress.highestPromisedHighestCommitted.set(data.progress, (lastCommitted.number, lastCommitted))
      val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None) {
        override def freshTimeout(interval: Long): Long = 1234L
      })
      fsm.underlyingActor.setAgent(Leader, data.copy(progress = committed))

      // when it gets one accept giving it a majority
      fsm ! AcceptNack(id99, 1, initialData.progress)
      fsm ! AcceptNack(id99, 2, initialData.progress)

      // it does not reply
      expectNoMsg(25 millisecond)
      // and returns to follower
      assert(fsm.underlyingActor.role == Follower)
      // and deletes pending leader work
      assert(fsm.underlyingActor.data.acceptResponses.isEmpty)
      assert(fsm.underlyingActor.data.epoch == None)
      assert(fsm.underlyingActor.data.prepareResponses.isEmpty)
      assert(fsm.underlyingActor.data.clientCommands.isEmpty)
      // and sends an exception to the clients
      client1.expectMsg(10 millisecond, NoLongerLeaderException(0, 1L))
      client2.expectMsg(10 millisecond, NoLongerLeaderException(0, 2L))
      // and sets a fresh timeout
      fsm.underlyingActor.data.timeout shouldBe 1234L
    }

    "commit in order when responses arrive out of order" in {
      // given a journal which records the save time and saved progress.
      var saveTime = 0L
      var savedProgress: Option[Progress] = None
      val stubJournal: Journal = stub[Journal]
      val delgatingJournal = new DelegatingJournal(stubJournal) {
        override def save(progress: Progress): Unit = {
          saveTime = System.nanoTime()
          savedProgress = Option(progress)
        }
      }
      // and accepts 99 and 100 are in the store with 98 committed
      val lastCommitted = Identifier(0, epoch, 98L)
      val id99 = Identifier(0, epoch, 99L)
      val a99 = Accept(id99, ClientRequestCommandValue(0, expectedBytes))
      (stubJournal.accepted _) when (99L) returns Some(a99)

      val id100 = Identifier(0, epoch, 100L)
      val a100 = Accept(id100, ClientRequestCommandValue(0, expectedBytes))
      (stubJournal.accepted _) when (100L) returns Some(a100)

      val votes = TreeMap(id99 -> AcceptResponsesAndTimeout(0L, a99, Map(0 -> AcceptAck(id99, 0, initialData.progress)))) + (id100 -> AcceptResponsesAndTimeout(0L, a100, Map(0 -> AcceptAck(id100, 0, initialData.progress))))

      val responses = acceptResponsesLens.set(initialData, votes)

      val committed = Progress.highestPromisedHighestCommitted.set(responses.progress, (lastCommitted.number, lastCommitted))

      // and an actor which records the send time
      var sendTime = 0L
      val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, delgatingJournal, ArrayBuffer.empty, None) {
        override def broadcast(msg: PaxosMessage): Unit = {
          sendTime = System.nanoTime()
          super.broadcast(msg)
        }
      })
      fsm.underlyingActor.setAgent(Leader, responses.copy(progress = committed))

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
      assert(saveTime > 0L)
      assert(sendTime > 0L)
      assert(saveTime < sendTime)

      // and delivered both values
      assert(fsm.underlyingActor.delivered.size == 2)
      // updated bookwork
      assert(fsm.underlyingActor.data.progress.highestCommitted.logIndex == 100)
      // and journal bookwork
      savedProgress match {
        case Some(p: Progress) => p == fsm.underlyingActor.data.progress
        case x => fail(x.toString)
      }
      // and deletes the pending work
      assert(fsm.underlyingActor.data.acceptResponses.size == 0)
    }

    "backdown when it sees a higher committed watermark in a response" in {
      val stubJournal: Journal = stub[Journal]
      // given accepts 99 and 100 are in the store can have committed up to 97
      val lastCommitted = Identifier(0, epoch, 97L)
      val id99 = Identifier(0, epoch, 99L)
      val a99 = Accept(id99, ClientRequestCommandValue(0, expectedBytes))
      (stubJournal.accepted _) when (99L) returns Some(a99)

      val id100 = Identifier(0, epoch, 100L)
      val a100 = Accept(id100, ClientRequestCommandValue(0, expectedBytes))
      (stubJournal.accepted _) when (100L) returns Some(a100)

      val votes = TreeMap(id99 -> AcceptResponsesAndTimeout(0L, a99, Map(0 -> AcceptAck(id99, 0, initialData.progress)))) + (id100 -> AcceptResponsesAndTimeout(0L, a100, Map(0 -> AcceptAck(id100, 0, initialData.progress))))

      val responses = acceptResponsesLens.set(initialData, votes)

      // and we have committed up to 97
      val committed = Progress.highestPromisedHighestCommitted.set(responses.progress, (lastCommitted.number, lastCommitted))

      val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None))
      fsm.underlyingActor.setAgent(Leader, responses.copy(progress = committed))

      // when it gets one accept giving it a majority on 100 but showing a higher commit mark
      fsm ! AcceptAck(id100, 1, Progress(epoch, Identifier(0, epoch, 98L)))

      // it does not send a message
      expectNoMsg(25 millisecond)

      // and becomes a follower
      fsm.underlyingActor.role should be(Follower)
      assert(fsm.underlyingActor.data.acceptResponses.isEmpty)
    }

    "rebroadcasts its commit with a fresh heartbeat when it gets a prompt from the scheduler" in {
      val stubJournal: Journal = stub[Journal]
      // given a leader
      val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, self, stubJournal, ArrayBuffer.empty, None))
      fsm.underlyingActor.setAgent(Leader, initialData)
      // when it gets a tick
      fsm ! HeartBeat
      // then it issues a heartbeat commit
      var commit: Commit = null
      expectMsgPF(100 millisecond) {
        case c: Commit => commit = c
      }
      val hb1 = commit.heartbeat
      // when it gets another tick
      Thread.sleep(2L)
      fsm ! HeartBeat
      // it issues a fresh heartbeat commit
      expectMsgPF(100 millisecond) {
        case c: Commit => commit = c
      }
      val hb2 = commit.heartbeat
      assert(hb2 > hb1)
    }

    "reissue same accept messages it gets a timeout and no challenge" in {
      resendsSameAcceptOnTimeoutNoOtherInfo(Leader)
    }

    "reissue higher accept messages upon learning of another nodes higher promise in a nack" in {
      resendsHigherAcceptOnLearningOtherNodeHigherPromise(Leader)
    }

    "reissues higher accept message upon having made a higher promise itself by the timeout" in {
      resendsHigherAcceptOnHavingMadeAHigherPromiseAtTimeout(Leader)
    }

    "backs down to be a follower when it makes a higher promise to another node" in {
      expectNoMsg(10 millisecond)
      val stubJournal: Journal = stub[Journal]

      // given low initial state committed up to 97
      val low = BallotNumber(5, 1)
      val initialData = PaxosData(Progress(low, minIdentifier), leaderHeartbeat2, timeout4, clusterSize5, TreeMap(), None, TreeMap(), Map.empty[Identifier, (CommandValue, ActorRef)])
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
      val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize5), 0, self, stubJournal, ArrayBuffer.empty, None) {
        override def clock() = timenow
      })
      fsm.underlyingActor.setAgent(Leader, initialData.copy(epoch = Some(epoch), progress = committed))
      // when it gets the high prepare
      fsm ! highPrepare
      // then it responds with a ack
      expectMsgPF(250 millisecond) {
        case ack: PrepareAck =>
      }
      // and returns to be follower
      assert(fsm.underlyingActor.role == Follower)
      // and clears data
      assert(fsm.underlyingActor.data.acceptResponses.isEmpty)
      assert(fsm.underlyingActor.data.prepareResponses.isEmpty)
      assert(fsm.underlyingActor.data.epoch == None)
      // and sets a fresh timeout
      assert(fsm.underlyingActor.data.timeout > 0 && fsm.underlyingActor.data.timeout - timenow < config.getLong(PaxosActor.leaderTimeoutMaxKey))
    }
  }

  // TODO ensure that when we leave a given state we gc any state not needed for the state we are jumping to. suggests one long lens for all the optional stuff

  // TODO get the underlying actor ref and check its illegal argument states
}
