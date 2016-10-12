package com.github.trex_paxos.library

import java.util.concurrent.atomic.AtomicBoolean

import TestHelpers._
import Ordering._

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer

class FollowerTests extends AllRolesTests {

  class ClockPaxosIO(time: Long) extends UndefinedIO {
    override def clock: Long = time
  }

  val initialDataAgent = PaxosAgent(0, Follower, initialData, initialQuorumStrategy)

  object `The FollowingFunction` {

    val paxosAlgorithm = new PaxosAlgorithm

    val commit = Commit(Identifier(0, BallotNumber(0, 0), 0))

    def `should be defined for a follower and a commit message` {
      assert(paxosAlgorithm.followingFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, commit)))
    }

    def `should not be defined for a recoverer and a commit message` {
      assert(!paxosAlgorithm.followingFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent.copy(role = Recoverer), commit)))
    }

    def `should not be defined for a leader and a commit message` {
      assert(!paxosAlgorithm.followingFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent.copy(role = Leader), commit)))
    }

    def `should be defined for a follower with an empty prepare response timed out and a CheckTimeout` {
      val dataNoPrepareResponsesAndTimout10 = initialDataAgent.data.copy(timeout = 10L)
      assert(paxosAlgorithm.followingFunction.isDefinedAt(PaxosEvent(new ClockPaxosIO(11L), initialDataAgent.copy(data = dataNoPrepareResponsesAndTimout10), CheckTimeout)))
    }

    def `should not be defined for a follower with an empty prepare response not timed out and a CheckTimeout` {
      val dataNoPrepareResponsesAndTimout10 = initialDataAgent.data.copy(timeout = 12L)
      assert(!paxosAlgorithm.followingFunction.isDefinedAt(PaxosEvent(new ClockPaxosIO(11L), initialDataAgent.copy(data = dataNoPrepareResponsesAndTimout10), CheckTimeout)))
    }

    def `should not be defined for a leader with an empty prepare response timed out and a CheckTimeout` {
      val dataNoPrepareResponsesAndTimout10 = initialDataAgent.data.copy(timeout = 10L)
      assert(!paxosAlgorithm.followingFunction.isDefinedAt(PaxosEvent(new ClockPaxosIO(11L), initialDataAgent.copy(data = dataNoPrepareResponsesAndTimout10, role = Leader), CheckTimeout)))
    }

    def `should not be defined for a recoverer with an empty prepare response timed out and a CheckTimeout` {
      val dataNoPrepareResponsesAndTimout10 = initialDataAgent.data.copy(timeout = 10L)
      assert(!paxosAlgorithm.followingFunction.isDefinedAt(PaxosEvent(new ClockPaxosIO(11L), initialDataAgent.copy(data = dataNoPrepareResponsesAndTimout10, role = Recoverer), CheckTimeout)))
    }

    def `should be defined for a follower with a non empty prepare response timed out and a CheckTimeout` {
      val dataNoPrepareResponsesAndTimout10 = initialDataAgent.data.copy(timeout = 10L, prepareResponses = TreeMap(Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), 0) -> Map.empty))
      assert(paxosAlgorithm.followingFunction.isDefinedAt(PaxosEvent(new ClockPaxosIO(11L), initialDataAgent.copy(data = dataNoPrepareResponsesAndTimout10), CheckTimeout)))
    }

    def `should not be defined for a follower with a non empty prepare response not timed out and a CheckTimeout` {
      val dataNoPrepareResponsesAndTimout10 = initialDataAgent.data.copy(timeout = 12L, prepareResponses = TreeMap(Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), 0) -> Map.empty))
      assert(!paxosAlgorithm.followingFunction.isDefinedAt(PaxosEvent(new ClockPaxosIO(11L), initialDataAgent.copy(data = dataNoPrepareResponsesAndTimout10), CheckTimeout)))
    }

    def `should not be defined for a recoverer with a non empty prepare response timed out and a CheckTimeout` {
      val dataNoPrepareResponsesAndTimout10 = initialDataAgent.data.copy(timeout = 10L, prepareResponses = TreeMap(Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), 0) -> Map.empty))
      assert(!paxosAlgorithm.followingFunction.isDefinedAt(PaxosEvent(new ClockPaxosIO(11L), initialDataAgent.copy(data = dataNoPrepareResponsesAndTimout10, role = Recoverer), CheckTimeout)))
    }

    def `should not be defined for a leader with a non empty prepare response timed out and a CheckTimeout` {
      val dataNoPrepareResponsesAndTimout10 = initialDataAgent.data.copy(timeout = 10L, prepareResponses = TreeMap(Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), 0) -> Map.empty))
      assert(!paxosAlgorithm.followingFunction.isDefinedAt(PaxosEvent(new ClockPaxosIO(11L), initialDataAgent.copy(data = dataNoPrepareResponsesAndTimout10, role = Leader), CheckTimeout)))
    }

    def `should be defined for a follower with a non empty prepare responses and a PrepareResponse` {
      val dataPrepareResponses = initialDataAgent.data.copy(prepareResponses = TreeMap(Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), 0) -> Map.empty))
      assert(paxosAlgorithm.followingFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent.copy(data = dataPrepareResponses), undefinedPrepareResponse)))
    }

    def `should not be defined for a recoverer with a non empty prepare responses and a PrepareResponse` {
      val dataPrepareResponses = initialDataAgent.data.copy(prepareResponses = TreeMap(Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), 0) -> Map.empty))
      assert(!paxosAlgorithm.followingFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent.copy(data = dataPrepareResponses, role = Recoverer), undefinedPrepareResponse)))
    }

    def `should not be defined for a leader with a non empty prepare responses and a PrepareResponse` {
      val dataPrepareResponses = initialDataAgent.data.copy(prepareResponses = TreeMap(Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), 0) -> Map.empty))
      assert(!paxosAlgorithm.followingFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent.copy(data = dataPrepareResponses, role = Leader), undefinedPrepareResponse)))
    }

    def `should be defined for a follower with an empty prepare responses and a PrepareResponse` {
      assert(paxosAlgorithm.followingFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, undefinedPrepareResponse)))
    }

    def `should not be defined for a recoverer with an empty prepare responses and a PrepareResponse` {
      assert(!paxosAlgorithm.followingFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent.copy(role = Recoverer), undefinedPrepareResponse)))
    }

    def `should not be defined for a leader with an empty prepare responses and a PrepareResponse` {
      assert(!paxosAlgorithm.followingFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent.copy(role = Leader), undefinedPrepareResponse)))
    }

    def `should be defined for a follower and an AcceptResponse` {
      assert(paxosAlgorithm.followingFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, undefinedAcceptResponse)))
    }

    def `should not be defined for a recoverer and an AcceptResponse` {
      assert(!paxosAlgorithm.followingFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent.copy(role = Recoverer), undefinedAcceptResponse)))
    }

    def `should not be defined for a leader and an AcceptResponse` {
      assert(!paxosAlgorithm.followingFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent.copy(role = Leader), undefinedAcceptResponse)))
    }
  }

  object `The Follower Function` {
    val paxosAlgorithm = new PaxosAlgorithm
    def `should be defined for a client command message` {
      assert(paxosAlgorithm.followerFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, DummyCommandValue("0"))))
    }

    def `should be defined for RetransmitRequest` {
      assert(paxosAlgorithm.followerFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, RetransmitRequest(0, 1, 0L))))
    }

    def `should be defined for a RetransmitResponse` {
      assert(paxosAlgorithm.followerFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, RetransmitResponse(0, 1, Seq(), Seq()))))
    }

    def `should be defined for a Prepare if the prepare is less than the promise` {
      assert(paxosAlgorithm.followerFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Prepare(Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), 0)))))
    }

    def `should be defined for a Prepare if the prepare is higher than the promise` {
      assert(paxosAlgorithm.followerFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Prepare(Identifier(0, BallotNumber(Int.MaxValue, Int.MaxValue), 0)))))
    }

    def `should be defined for a Prepare if the prepare is equal to the promise` {
      assert(paxosAlgorithm.followerFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Prepare(Identifier(0, initialDataAgent.data.progress.highestPromised, 0)))))
    }

    def `should be defined for an Accept with a lower number` {
      assert(paxosAlgorithm.followerFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Accept(Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), 0), NoOperationCommandValue))))
    }

    def `should be defined for an Accept with a higher number for a committed slot` {
      val promise = BallotNumber(Int.MaxValue, Int.MaxValue)
      val initialData = TestHelpers.highestPromisedHighestCommittedLens.set(TestHelpers.initialData, (promise, Identifier(from = 0, number = promise, logIndex = 99L)))
      val higherCommittedAgent = PaxosAgent(0, Follower, initialData, initialQuorumStrategy)
      assert(paxosAlgorithm.followerFunction.isDefinedAt(PaxosEvent(undefinedIO, higherCommittedAgent, Accept(Identifier(0, promise, 0), NoOperationCommandValue))))
    }

    def `should be defined for an Accept equal to promise` {
      val promise = BallotNumber(Int.MaxValue, Int.MaxValue)
      val initialData = highestPromisedLens.set(TestHelpers.initialData, promise)
      val equalPromiseAgent = PaxosAgent(0, Follower, initialData, initialQuorumStrategy)
      assert(paxosAlgorithm.followerFunction.isDefinedAt(PaxosEvent(undefinedIO, equalPromiseAgent, Accept(Identifier(0, promise, 0), NoOperationCommandValue))))
    }

    def `should be defined for an Accept greater than promise` {
      val higherAcceptId = BallotNumber(Int.MaxValue, Int.MaxValue)
      val lowerPromise = BallotNumber(Int.MaxValue -1 , Int.MaxValue - 1)
      val initialData = highestPromisedLens.set(TestHelpers.initialData, lowerPromise)
      val higherPromiseAgent = PaxosAgent(0, Follower, initialData, initialQuorumStrategy)
      assert(paxosAlgorithm.followerFunction.isDefinedAt(PaxosEvent(undefinedIO, higherPromiseAgent, Accept(Identifier(0, higherAcceptId, 0), NoOperationCommandValue))))
    }

    def `should be defined for a Heartbeat` = {
      val agent = PaxosAgent(0, Follower, initialData, initialQuorumStrategy)
      assert(paxosAlgorithm.followerFunction.isDefinedAt(PaxosEvent(undefinedIO, agent, HeartBeat)))
    }

    def `should be defined for a CheckTimeout when not timedout` {
      val agent = PaxosAgent(0, Follower, initialData, initialQuorumStrategy)
      assert(paxosAlgorithm.followerFunction.isDefinedAt(PaxosEvent(negativeClockIO, agent, CheckTimeout)))
    }
  }

  object `A Follower` {
    val paxosAlgorithm = new PaxosAlgorithm
    def `responds is not leader` {
      respondsIsNotLeader(Follower)
    }
    def `should use follower commit handler` {
      import CommitHandlerTests.a14
      // given
      val agent = PaxosAgent(0, Follower, initialData, initialQuorumStrategy)
      val event = new PaxosEvent(new UndefinedIO, agent, Commit(a14.id, initialData.leaderHeartbeat))
      val invoked = new AtomicBoolean(false)
      val paxosAlgorithm = new PaxosAlgorithm {
        override def handleFollowerCommit(io: PaxosIO, agent: PaxosAgent, c: Commit): PaxosAgent = {
          invoked.set(true)
          agent
        }
      }
      // when
      val PaxosAgent(_, _, data, _) = paxosAlgorithm(event)
      // then
      invoked.get() shouldBe true
    }
    def `should not change state if not timed out` {
      val agent = PaxosAgent(0, Follower, initialData, initialQuorumStrategy)
      val notTimedOutEvent = PaxosEvent(negativeClockIO, agent, CheckTimeout)
      val PaxosAgent(_, role, data, _) = paxosAlgorithm(notTimedOutEvent)
      assert( role == agent.role && data == agent.data)
    }
    def `should update its timeout and observed heartbeat when it sees a commit` {

      val timeout = 123L
      val heartbeat = 9999L

      val timeoutIO = new UndefinedIO with SilentLogging {
        override def randomTimeout: Long = timeout
      }

      val agent = PaxosAgent(0, Follower, initialData, initialQuorumStrategy)
      val event = PaxosEvent(timeoutIO, agent, Commit(Identifier(0, BallotNumber(lowValue, lowValue), 0L), heartbeat))
      val PaxosAgent(_, role, data, _) = paxosAlgorithm(event)
      assert( role == Follower && data == agent.data.copy(timeout = timeout, leaderHeartbeat = heartbeat))
    }
    def `should ignore a lower commit` {
      val agent = PaxosAgent(0, Follower, initialDataCommittedSlotOne, initialQuorumStrategy)
      val event = PaxosEvent(undefinedSilentIO, agent, Commit(Identifier(0, BallotNumber(lowValue, lowValue), 0L), Long.MinValue))
      val PaxosAgent(_, role, data, _) = paxosAlgorithm(event)
      role shouldBe Follower
      data shouldBe agent.data
    }
    def `should ignore a late prepare response` {
      shouldIngoreLatePrepareResponse(Leader)
    }
    def `should ignore an accept response`  {
      // given
      val agent = PaxosAgent(0, Follower, initialData, initialQuorumStrategy)
      val message = AcceptAck(initialData.progress.highestCommitted, 0, initialData.progress)
      val event = new PaxosEvent(new UndefinedIO, agent, message)
      val paxosAlgorithm = new PaxosAlgorithm
      // when
      val PaxosAgent(_, newRole, newData, _) = paxosAlgorithm(event)
      // then
      newRole shouldBe Follower
      newData shouldBe initialData
    }
    def `should update timeout and hearbeat up repeated commit` {
      // given
      val agent = PaxosAgent(0, Follower, initialData, initialQuorumStrategy)
      val message = Commit(initialData.progress.highestCommitted, Long.MaxValue)
      val io = new UndefinedIO with SilentLogging {
        override def randomTimeout: Long = 12345L
      }
      val event = new PaxosEvent(io, agent, message)
      val paxosAlgorithm = new PaxosAlgorithm
      // when
      val PaxosAgent(_, newRole, newData, _) = paxosAlgorithm(event)
      // then
      newRole shouldBe Follower
      newData shouldBe initialData.copy(timeout = 12345L, leaderHeartbeat = Long.MaxValue)
    }
    def `should time-out and send a low prepare` {
      val paxosAlgorithm = new PaxosAlgorithm
      // given an empty journal
      val stubJournal: Journal = stub[Journal]
      val bounds = JournalBounds(0L, 0L)
      (stubJournal.bounds _) when() returns (bounds)
      // and an io which has a high clock and checks the sent messages
      val messages: ArrayBuffer[PaxosMessage] = ArrayBuffer.empty
      val io = new UndefinedIO with SilentLogging {
        override def clock: Long = Long.MaxValue

        override def send(msg: PaxosMessage): Unit = messages += msg

        override def randomTimeout: Long = 987654L

        override def journal: Journal = stubJournal
      }
      // and a check timeout event
      val message = CheckTimeout
      val agent = PaxosAgent(0, Follower, initialData, initialQuorumStrategy)
      val event = new PaxosEvent(io, agent, message)
      // when we process the event
      val PaxosAgent(_, newRole, newData, _) = paxosAlgorithm(event)
      // then
      messages.headOption.value match {
        case p: Prepare if p == minPrepare => // good
        case f => fail(f.toString)
      }
      newRole shouldBe Follower
      newData.timeout shouldBe 987654L
      newData.prepareResponses.get(minPrepare.id) match {
        case Some(map) if map.size == 1 => // good
        case x => fail(x.toString)
      }
    }
    def `should backdown and issue retransmit response if another node has committed a higher slot` {
      val paxosAlgorithm = new PaxosAlgorithm
      // given a follower that has issued a min prepare
      val selfAck = PrepareAck(minPrepare.id, 0, initialData.progress, 0, 0, None)
      val prepareResponses = initialData.prepareResponses + (minPrepare.id -> Map(0 -> selfAck))
      val agent = PaxosAgent(0, Follower, initialData.copy(prepareResponses = prepareResponses), initialQuorumStrategy)
      // and an io that captures messages
      val messages: ArrayBuffer[PaxosMessage] = ArrayBuffer.empty
      val io = new UndefinedIO with SilentLogging{
        override def send(msg: PaxosMessage): Unit = messages += msg
        override def randomTimeout: Long = 987654L
      }

      // and a higher committed slot nack to the min prepare
      val message = PrepareNack(minPrepare.id, 2, Progress.highestCommittedLens.set(initialData.progress, initialData.progress.highestCommitted.copy(logIndex = Long.MaxValue)), 0, Long.MinValue)
      val event = new PaxosEvent(io, agent, message)
      // when
      val PaxosAgent(_, newRole, newData, _) = paxosAlgorithm(event)
      // then
      newRole shouldBe Follower
      newData.prepareResponses.isEmpty shouldBe true
      newData.timeout shouldBe 987654L
    }
    def `should backdown if another node has seen a higher leader heartbeat where leader has majority` {
      val paxosAlgorithm = new PaxosAlgorithm
      // given a follower in a cluster sized 3 that has issued a min prepare
      val selfAck = PrepareAck(minPrepare.id, 0, initialData.progress, 0, 0, None)
      val prepareResponses = initialData.prepareResponses + (minPrepare.id -> Map(0 -> selfAck))
      val agent = PaxosAgent(0, Follower, initialData.copy(prepareResponses = prepareResponses), initialQuorumStrategy)
      // and an io that captures messages
      val messages: ArrayBuffer[PaxosMessage] = ArrayBuffer.empty
      val io = new UndefinedIO with SilentLogging{
        override def send(msg: PaxosMessage): Unit = messages += msg
        override def randomTimeout: Long = 987654L
      }

      // and a nack that indicates a leader behind a network partition
      val message = PrepareNack(minPrepare.id, 2, initialData.progress, 0, Long.MaxValue)
      val event = new PaxosEvent(io, agent, message)
      // when
      val PaxosAgent(_, newRole, newData, _) = paxosAlgorithm(event)
      // then
      newRole shouldBe Follower
      newData.prepareResponses.isEmpty shouldBe true
      newData.timeout shouldBe 987654L
      newData.leaderHeartbeat shouldBe Long.MaxValue
    }
    def `should backdown if sees a commit from another leader` {
      val paxosAlgorithm = new PaxosAlgorithm
      // given a follower in a cluster sized 3 that has issued a min prepare
      val selfAck = PrepareAck(minPrepare.id, 0, initialData.progress, 0, 0, None)
      val prepareResponses = initialData.prepareResponses + (minPrepare.id -> Map(0 -> selfAck))
      val agent = PaxosAgent(0, Follower, initialData.copy(prepareResponses = prepareResponses), initialQuorumStrategy)
      // and an io that captures messages
      val messages: ArrayBuffer[PaxosMessage] = ArrayBuffer.empty
      val io = new UndefinedIO with SilentLogging{
        override def send(msg: PaxosMessage): Unit = messages += msg
        override def randomTimeout: Long = 987654L
      }

      // then when it receives a fresh heartbeat commit
      val message = Commit(Identifier(1, BallotNumber(lowValue + 1, lowValue), 0), 0)
      val event = new PaxosEvent(io, agent, message)
      // when
      val PaxosAgent(_, newRole, newData, _) = paxosAlgorithm(event)
      // then
      newRole shouldBe Follower
      newData.prepareResponses.isEmpty shouldBe true
      newData.timeout shouldBe 987654L
    }
    def `should backdown if it sees a commit from same leader with higher heartbeat` {
      val paxosAlgorithm = new PaxosAlgorithm
      // given a follower in a cluster sized 3 that has issued a min prepare
      val selfAck = PrepareAck(minPrepare.id, 0, initialData.progress, 0, 0, None)
      val prepareResponses = initialData.prepareResponses + (minPrepare.id -> Map(0 -> selfAck))
      val agent = PaxosAgent(0, Follower, initialData.copy(prepareResponses = prepareResponses), initialQuorumStrategy)
      // and an io that captures messages
      val messages: ArrayBuffer[PaxosMessage] = ArrayBuffer.empty
      val io = new UndefinedIO with SilentLogging{
        override def send(msg: PaxosMessage): Unit = messages += msg
        override def randomTimeout: Long = 987654L
      }

      // then when it receives a fresh heartbeat commit
      val message = Commit(Identifier(0, BallotNumber(lowValue, lowValue), 0), Long.MaxValue)
      val event = new PaxosEvent(io, agent, message)
      // when
      val PaxosAgent(_, newRole, newData, _) = paxosAlgorithm(event)
      // then
      newRole shouldBe Follower
      newData.prepareResponses.isEmpty shouldBe true
      newData.timeout shouldBe 987654L
      newData.leaderHeartbeat shouldBe Long.MaxValue
    }
    def `should bootstrap from a retransmission response` {
      val paxosAlgorithm = new PaxosAlgorithm

      val tempJournal = new InMemoryJournal()

      // given some retransmitted committed values
      val v1 = DummyCommandValue("1")
      val v2 = DummyCommandValue("2")
      val v3 = DummyCommandValue("3")
      val a1 =
        Accept(Identifier(1, BallotNumber(1, 1), 1L), v1)
      val a2 =
        Accept(Identifier(2, BallotNumber(2, 2), 2L), v2)
      val a3 =
        Accept(Identifier(3, BallotNumber(3, 3), 3L), v3)
      val retransmission = RetransmitResponse(1, 0, Seq(a1, a2, a3), Seq.empty)

      // and a verifiable io
      val messages: ArrayBuffer[PaxosMessage] = ArrayBuffer()
      val delivered: ArrayBuffer[DummyCommandValue] = ArrayBuffer()
      val io = new UndefinedIO with SilentLogging{
        override def send(msg: PaxosMessage): Unit = messages += msg

        override def randomTimeout: Long = 987654L

        override def deliver(payload: Payload): Any = payload match {
          case Payload(_, c: DummyCommandValue) => delivered += c
          case _ =>
        }

        override def journal: Journal = tempJournal
      }

      // and an empty node
      val agent = PaxosAgent(0, Follower, initialData, initialQuorumStrategy)
      // when the retransmission is received
      val PaxosAgent(_, role, data, _) = paxosAlgorithm(new PaxosEvent(io, agent, retransmission))

      // then it sends no messages
      messages.isEmpty shouldBe true
      // stays in state
      role shouldBe Follower
      // updates its commit index
      data.progress.highestCommitted shouldBe a3.id

      // delivered the committed values
      delivered.size should be(3)
      delivered(0) should be(v1)
      delivered(1) should be(v2)
      delivered(2) should be(v3)

      // and journalled the values so that it can retransmit itself
      tempJournal.bounds shouldBe JournalBounds(1, 3)

      tempJournal.accepted(1) match {
        case Some(a) if a.id == a1.id => // good
        case x => fail(x.toString)
      }
      tempJournal.accepted(2) match {
        case Some(a) if a.id == a2.id => // good
        case x => fail(x.toString)
      }
      tempJournal.accepted(3) match {
        case Some(a) if a.id == a3.id => // good
        case x => fail(x.toString)
      }

      tempJournal.p() match {
        case (_, Progress(_, a3.id)) => // good
        case f => fail(f.toString)
      }
    }
    def `should switch to recoverer and issue multiple prepares if there are slots to recover and no leader` {
      val paxosAlgorithm = new PaxosAlgorithm

      // given three uncommitted values in the journal

      val id1 = Identifier(0, BallotNumber(lowValue + 1, 0), 1)
      val a1 = Accept(id1, DummyCommandValue("1"))

      val id2 = Identifier(0, BallotNumber(lowValue + 1, 0), 2)
      val a2 = Accept(id2, DummyCommandValue("2"))

      val id3 = Identifier(0, BallotNumber(lowValue + 1, 0), 3)
      val a3 = Accept(id3, DummyCommandValue("3"))

      val tempJournal = new InMemoryJournal()
      tempJournal.a.put(1L , (0L, a1))
      tempJournal.a.put(2L , (0L, a2))
      tempJournal.a.put(3L , (0L, a3))

      // and a verifiable io
      val messages: ArrayBuffer[PaxosMessage] = ArrayBuffer()
      val delivered: ArrayBuffer[DummyCommandValue] = ArrayBuffer()
      val io = new UndefinedIO with SilentLogging{
        override def send(msg: PaxosMessage): Unit = messages += msg

        override def randomTimeout: Long = 987654L

        override def clock: Long = Int.MaxValue

        override def deliver(payload: Payload): Any = payload match {
          case Payload(_, c: DummyCommandValue) => delivered += c
          case _ =>
        }

        override def journal: Journal = tempJournal
      }

      // and an empty node
      val agent = PaxosAgent(0, Follower, initialData, initialQuorumStrategy)
      // when at timeout is received
      val follower = paxosAlgorithm(new PaxosEvent(io, agent, CheckTimeout))
      // then it sends out a single low prepare
      messages.headOption.value match {
        case `minPrepare` => // good
        case f => fail(f.toString)
      }
      messages.clear()
      // when it hears that the other follower hsa no fresh heartbeat
      val nack = PrepareNack(minPrepare.id, 2, initialData.progress, initialData.progress.highestCommitted.logIndex, initialData.leaderHeartbeat)
      val recoverer = paxosAlgorithm(new PaxosEvent(io, follower, nack))

      // it sends out prepares for each uncommitted slot plus an extra slot
      messages.headOption.value match {
        case Prepare(id1) => // good
        case f => fail(f.toString)
      }
      messages.drop(1)
      messages.headOption.value match {
        case Prepare(id2) => // good
        case f => fail(f.toString)
      }
      messages.drop(1)
      messages.headOption.value match {
        case Prepare(id3) => // good
        case f => fail(f.toString)
      }
      val id4 = Identifier(0, BallotNumber(lowValue + 1, 0), 4)
      messages.drop(1)
      messages.headOption.value match {
        case Prepare(id4) => // good
        case f => fail(f.toString)
      }

      // and promotes to candidate
      recoverer.role shouldBe Recoverer
      // and sets a new timeout
      recoverer.data.timeout shouldBe 987654L
      // and make a promise to self
      recoverer.data.progress.highestPromised shouldBe id1.number
      // and votes for its own prepares
      recoverer.data.prepareResponses.isEmpty shouldBe false
      val prapareIds = recoverer.data.prepareResponses map {
        case (id, map) if map.keys.headOption == Some(0) && map.values.headOption.getOrElse(fail).requestId == id =>
          id
        case x => fail(x.toString)
      }
      assert(true == prapareIds.toSet.contains(id1))
      assert(true == prapareIds.toSet.contains(id2))
      assert(true == prapareIds.toSet.contains(id3))
    }
  }
}
