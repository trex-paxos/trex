package com.github.trex_paxos.library

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import TestHelpers._

import scala.collection.immutable.{SortedMap, TreeMap}
import Ordering._

import scala.collection.mutable.ArrayBuffer

class LeaderTests extends AllRolesTests with LeaderLikeTests {

  object `The Leader Function` {
    val initialDataAgent = PaxosAgent(0, Leader, initialData, initialQuorumStrategy)
    def `should be defined for a leader and RetransmitRequest` {
      assert(paxosAlgorithm.leaderFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, RetransmitRequest(0, 1, 0L))))
    }

    def `should be defined for a leader and RetransmitResponse` {
      assert(paxosAlgorithm.leaderFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, RetransmitResponse(0, 1, Seq(), Seq()))))
    }

    def `should be defined for a leader and a Prepare if the prepare is less than the promise` {
      assert(paxosAlgorithm.leaderFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Prepare(Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), 0)))))
    }

    def `should be defined for a leader and a Prepare if the prepare is higher than the promise` {
      assert(paxosAlgorithm.leaderFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Prepare(Identifier(0, BallotNumber(Int.MaxValue, Int.MaxValue), 0)))))
    }

    def `should be defined for a leader and a Prepare if the prepare is equal to the promise` {
      assert(paxosAlgorithm.leaderFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Prepare(Identifier(0, initialDataAgent.data.progress.highestPromised, 0)))))
    }

    def `should be defined for a leader and an Accept with a lower number` {
      assert(paxosAlgorithm.leaderFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Accept(Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), 0), NoOperationCommandValue))))
    }

    def `should be defined for a leader and an Accept with a higher number for a committed slot` {
      val promise = BallotNumber(Int.MaxValue, Int.MaxValue)
      val initialData = TestHelpers.highestPromisedHighestCommittedLens.set(TestHelpers.initialData, (promise, Identifier(from = 0, number = promise, logIndex = 99L)))
      val higherCommittedAgent = PaxosAgent(0, Leader, initialData, initialQuorumStrategy)
      assert(paxosAlgorithm.leaderFunction.isDefinedAt(PaxosEvent(undefinedIO, higherCommittedAgent, Accept(Identifier(0, promise, 0), NoOperationCommandValue))))
    }

    def `should be defined for an Accept equal to promise` {
      val promise = BallotNumber(Int.MaxValue, Int.MaxValue)
      val initialData = highestPromisedLens.set(TestHelpers.initialData, promise)
      val equalPromiseAgent = PaxosAgent(0, Leader, initialData, initialQuorumStrategy)
      assert(paxosAlgorithm.leaderFunction.isDefinedAt(PaxosEvent(undefinedIO, equalPromiseAgent, Accept(Identifier(0, promise, 0), NoOperationCommandValue))))
    }

    def `should be defined for an Accept greater than promise` {
      val higherAcceptId = BallotNumber(Int.MaxValue, Int.MaxValue)
      val lowerPromise = BallotNumber(Int.MaxValue -1 , Int.MaxValue - 1)
      val initialData = highestPromisedLens.set(TestHelpers.initialData, lowerPromise)
      val higherPromiseAgent = PaxosAgent(0, Leader, initialData, initialQuorumStrategy)
      assert(paxosAlgorithm.leaderFunction.isDefinedAt(PaxosEvent(undefinedIO, higherPromiseAgent, Accept(Identifier(0, higherAcceptId, 0), NoOperationCommandValue))))
    }

    def `should be defined for a Heartbeat` = {
      val agent = PaxosAgent(0, Leader, initialData, initialQuorumStrategy)
      assert(paxosAlgorithm.leaderFunction.isDefinedAt(PaxosEvent(undefinedIO, agent, HeartBeat)))
    }

    def `should be defined for a CheckTimeout when not timed out` = {
      val agent = PaxosAgent(0, Leader, initialData, initialQuorumStrategy)
      assert(paxosAlgorithm.leaderFunction.isDefinedAt(PaxosEvent(negativeClockIO, agent, CheckTimeout)))
    }

    def `should be defined for a CheckTimeout when accepts are timed out` {
      val agent = PaxosAgent(0, Leader, initialData.copy(acceptResponses = emptyAcceptResponses), initialQuorumStrategy)
      assert(paxosAlgorithm.leaderFunction.isDefinedAt(PaxosEvent(maxClockIO, agent, CheckTimeout)))
    }

    def `should deal with timed-out prepares before timed-out accepts` {
      val handleResendAcceptsInvoked = Box(false)
      val handleResendPreparesInvoked = Box(false)
      val paxosAlgorithm = new PaxosAlgorithm {
        override def handleResendAccepts(io: PaxosIO, agent: PaxosAgent, time: Long): PaxosAgent = {
          handleResendAcceptsInvoked(true)
          agent
        }
        override def handleResendPrepares(io: PaxosIO, agent: PaxosAgent, time: Long): PaxosAgent = {
          handleResendPreparesInvoked(true)
          agent
        }
      }
      val agent = PaxosAgent(0, Leader, initialData.copy(prepareResponses = prepareSelfVotes, acceptResponses = emptyAcceptResponses), initialQuorumStrategy)
      paxosAlgorithm.leaderFunction(PaxosEvent(maxClockIO, agent, CheckTimeout))
      assert(handleResendPreparesInvoked() == true && handleResendAcceptsInvoked() == false)
    }

    def `should be defined for a commit at a higher log index` {
      val agent = PaxosAgent(0, Leader, initialData, initialQuorumStrategy)
      val commit = Commit(Identifier(1, initialData.progress.highestPromised, Int.MaxValue))
      assert(paxosAlgorithm.leaderFunction.isDefinedAt(PaxosEvent(maxClockIO, agent, commit)))
    }

    def `should be defined for a commit at a same log index with a higher number` {
      val agent = PaxosAgent(0, Leader, initialData, initialQuorumStrategy)
      val commit = Commit(Identifier(1, BallotNumber(Int.MaxValue, Int.MaxValue), initialData.progress.highestCommitted.logIndex))
      assert(paxosAlgorithm.leaderFunction.isDefinedAt(PaxosEvent(maxClockIO, agent, commit)))
    }

    def `should be defined for a low commit` {
      val agent = PaxosAgent(0, Leader, initialData, initialQuorumStrategy)
      val commit = Commit(Identifier(1, BallotNumber(Int.MinValue, Int.MinValue), Long.MinValue))
      assert(paxosAlgorithm.leaderFunction.isDefinedAt(PaxosEvent(maxClockIO, agent, commit)))
    }

    def `should be defined for Heatbeat`{
      val agent = PaxosAgent(0, Leader, initialData, initialQuorumStrategy)
      assert(paxosAlgorithm.leaderFunction.isDefinedAt(PaxosEvent(maxClockIO, agent, HeartBeat)))
    }

    def `should be defined for a client command message` {
      assert(paxosAlgorithm.leaderFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, DummyCommandValue(0))))
    }

    def `should be defined for a late PrepareResponse`  {
      val dataPrepareResponses = initialDataAgent.data.copy(prepareResponses = TreeMap(Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), 0) -> Map.empty))
      assert(paxosAlgorithm.leaderFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent.copy(data = dataPrepareResponses), undefinedPrepareResponse)))
    }

    val leader = PaxosAgent(0, Leader, initialDataAgent.data.copy(
      epoch = Option(BallotNumber(Int.MaxValue, Int.MaxValue)),
      acceptResponses = acceptSelfAck98,
      clientCommands = initialDataClientCommand
    ), initialQuorumStrategy)

    def `Return to follower handler should do nothing for commit not at higher slot ` = {
      val handler = new ReturnToFollowerHandler with CommitHandler {}
      val commitAtIndex = Commit(leader.data.progress.highestCommitted)
      handler.handleReturnToFollowerOnHigherCommit(undefinedSilentIO, leader, commitAtIndex) match {
        case `leader` => // good
        case f => fail(f.toString)
      }
    }
  }

  object `A Leader` {
    def `should ignore a lower commit` {
      shouldIngoreLowerCommit(Leader)
    }
    def `should ignore a late prepare response` {
      shouldIngoreLatePrepareResponse(Leader)
    }
    def `should ingore a commit for same slot from lower numbered node` {
      shouldIngoreCommitMessageSameSlotLowerNodeId(Leader)
    }
    def `should backdown on commit same slot higher node number` {
      shouldBackdownOnCommitSameSlotHigherNodeId(Leader)
    }
    def `should backdown on higher slot commit` {
      shouldBackdownOnHigherSlotCommit(Leader)
    }
    def `should backdown and commit on higher slot commit` {
      shouldBackdownAndCommitOnHigherSlotCommit(Leader)
    }
    def `reissue same accept messages it gets a timeout and no challenge` {
      shouldReissueSameAcceptMessageIfTimeoutNoChallenge(Leader)
    }
    def `reissue higher accept messages upon learning of another nodes higher promise in a nack` {
      sendHigherAcceptOnLearningOtherNodeHigherPromise(Leader)
    }
    def `reissues higher accept message upon having made a higher promise itself by the timeout` {
      sendsHigherAcceptOnHavingMadeAHigherPromiseAtTimeout(Leader)
    }
    val epoch = BallotNumber(1, 1)
    val dataNewEpoch = epochLens.set(initialData, Some(epoch))
    val freshAcceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout] = SortedMap.empty[Identifier, AcceptResponsesAndTimeout](Ordering.IdentifierLogOrdering)
    val initialLeaderData = leaderLens.set(dataNewEpoch, (SortedMap.empty[Identifier, Map[Int, PrepareResponse]], freshAcceptResponses, Map.empty))
    val agentInitialLeaderData = new PaxosAgent(0, Leader, initialLeaderData, initialQuorumStrategy)
    val expectedString2 = "Paxos"
    val expectedBytes2 = expectedString2.getBytes

    def `broadcast client values` {
      // given some client commands
      val expectedString3 = "Lamport"
      val expectedBytes3 = expectedString3.getBytes
      val c1 = DummyCommandValue(1)
      val c2 = DummyCommandValue(2)
      val c3 = DummyCommandValue(3)
      // and a verifiable IO
      val stubJournal: Journal = stub[Journal]
      val sent = ArrayBuffer[PaxosMessage]()
      val io = new UndefinedIO with SilentLogging {
        override def journal: Journal = stubJournal

        override def send(msg: PaxosMessage): Unit = sent += msg

        override def senderId: String = 1.toString

        override def randomTimeout: Long = 12345L
      }
      val agent1 = paxosAlgorithm(new PaxosEvent(io, agentInitialLeaderData, c1))
      val agent2 = paxosAlgorithm(new PaxosEvent(io, agent1, c2))
      val agent3 = paxosAlgorithm(new PaxosEvent(io, agent2, c3))
      // then we expect three accepts to be broadcast
      val accept1 = Accept(Identifier(0, epoch, 1L), c1)
      val accept2 = Accept(Identifier(0, epoch, 2L), c2)
      val accept3 = Accept(Identifier(0, epoch, 3L), c3)
      sent.headOption.value match {
        case `accept1` => // good
        case f => fail(f.toString)
      }
      sent.takeRight(2).headOption.value match {
        case `accept2` => // good
        case f => fail(f.toString)
      }
      sent.takeRight(1).headOption.value match {
        case `accept3` => // good
        case f => fail(f.toString)
      }
      // and it says as leader
      agent3.role shouldBe Leader
      // and is read to record responses
      agent3.data.acceptResponses.size shouldBe 3
      // and holds slots in order
      agent3.data.acceptResponses.keys.headOption match {
        case Some(id) => assert(id.logIndex == 1)
        case x => fail(x.toString)
      }
      agent3.data.acceptResponses.keys.lastOption match {
        case Some(id) => assert(id.logIndex == 3)
        case x => fail(x.toString)
      }
      // and has journaled the values
      (stubJournal.accept _).verify(Seq(accept1))
      (stubJournal.accept _).verify(Seq(accept2))
      (stubJournal.accept _).verify(Seq(accept3))
      // and holds the values
      agent3.data.clientCommands.size shouldBe 3
      agent3.data.clientCommands(accept1.id) shouldBe (c1 -> "1")
      agent3.data.clientCommands(accept2.id) shouldBe (c2 -> "1")
      agent3.data.clientCommands(accept3.id) shouldBe (c3 -> "1")
    }
    def `commits when it receives a majority of accept acks` {
      // given a verifiable io
      val lastDelivered = new AtomicReference[CommandValue]()
      val inMemoryJournal = new InMemoryJournal
      val sent: ArrayBuffer[PaxosMessage] = ArrayBuffer()
      val sentTime = new AtomicLong()
      val io = new UndefinedIO with SilentLogging {
        override def journal: Journal = inMemoryJournal

        override def clock: Long = Long.MaxValue

        override def randomTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = {
          sentTime.set(System.nanoTime())
          sent += msg
        }

        override def deliver(payload: Payload): Any = {
          lastDelivered.set(payload.command)
          value
        }
      }
      // and a leader who has committed slot 98 and broadcast slot 99
      val lastCommitted = Identifier(0, epoch, 98L)
      val id99 = Identifier(0, epoch, 99L)
      val a99 = Accept(id99, DummyCommandValue(99))
      val votes = TreeMap(id99 -> AcceptResponsesAndTimeout(0L, a99, Map(0 -> AcceptAck(id99, 0, initialData.progress))))
      val responses = acceptResponsesLens.set(initialData, votes)
      val committed = Progress.highestPromisedHighestCommitted.set(responses.progress, (lastCommitted.number, lastCommitted))
      val data = responses.copy(progress = committed)
      val agent = PaxosAgent(0, Leader, data, initialQuorumStrategy)
      val accept = Accept(id99, DummyCommandValue(99))
      inMemoryJournal.a.put(99L, (0L -> accept))
      // when it gets an ack giving it a majority
      val ack = AcceptAck(id99, 1, initialData.progress)
      val leader = paxosAlgorithm(new PaxosEvent(io, agent, ack))
      // it broadcasts the commit
      sent.headOption.value match {
        case Commit(prepareId, _) => // good
        case f => fail(f.toString)
      }
      // it delivers the value
      Option(lastDelivered.get()) match {
        case Some(DummyCommandValue(99)) => // good
        case f => fail(f.toString)
      }
      // and journal bookwork
     inMemoryJournal.p() match {
        case (_, p) if p == leader.data.progress => // good
        case x => fail(x.toString)
      }
      // and deletes the pending work
      assert(leader.data.acceptResponses.size == 0)
      val saveTime = inMemoryJournal.lastSaveTime.get()
      assert(saveTime > 0L && sentTime.get() > 0L && saveTime < sentTime.get())
    }
    def `commits in order when it receives responses out of order` {
      // given a verifiable io
      val delivered = ArrayBuffer[CommandValue]()
      val inMemoryJournal = new InMemoryJournal
      val sent: ArrayBuffer[PaxosMessage] = ArrayBuffer()
      val sentTime = new AtomicLong()
      val io = new UndefinedIO with SilentLogging {
        override def journal: Journal = inMemoryJournal

        override def clock: Long = Long.MaxValue

        override def randomTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = {
          sentTime.set(System.nanoTime())
          sent += msg
        }

        override def deliver(payload: Payload): Any = {
          delivered += payload.command
          value
        }
      }
      // and a leader who has committed slot 98 and broadcast slot 99 and 100
      val lastCommitted = Identifier(0, epoch, 98L)
      val id99 = Identifier(0, epoch, 99L)
      val a99 = Accept(id99, DummyCommandValue(99))
      val id100 = Identifier(0, epoch, 100L)
      val a100 = Accept(id100, DummyCommandValue(100))

      val votes = TreeMap(id99 -> AcceptResponsesAndTimeout(0L, a99, Map(0 -> AcceptAck(id99, 0, initialData.progress)))) +
        (id100 -> AcceptResponsesAndTimeout(0L, a100, Map(0 -> AcceptAck(id100, 0, initialData.progress))))

      val responses = acceptResponsesLens.set(initialData, votes)
      val committed = Progress.highestPromisedHighestCommitted.set(responses.progress, (lastCommitted.number, lastCommitted))
      val data = responses.copy(progress = committed)
      val agent = PaxosAgent(0, Leader, data, initialQuorumStrategy)
      val accept99 = Accept(id99, DummyCommandValue(99))
      inMemoryJournal.a.put(99L, (0L -> accept99))
      val accept100 = Accept(id100, DummyCommandValue(100))
      inMemoryJournal.a.put(100L, (0L -> accept100))
      // when it gets on accept giving it a majority but on 100 slot
      val ack1 = AcceptAck(id100, 1, initialData.progress)
      val leader1 = paxosAlgorithm(new PaxosEvent(io, agent, ack1))
      // then it does not send a commit
      sent.isEmpty shouldBe true
      // then when it gets the majority on the 99 slot
      val ack2 = AcceptAck(id99, 1, initialData.progress)
      val leader = paxosAlgorithm(new PaxosEvent(io, leader1, ack2))
      // it broadcasts the commit
      sent.headOption.value match {
        case Commit(id100, _) => // good
        case f => fail(f.toString)
      }
      // it delivers the values in order
      delivered.headOption.value match {
        case DummyCommandValue(99) => // good
        case f => fail(f.toString)
      }
      // it delivers the values in order
      delivered.tail.headOption.value match {
        case DummyCommandValue(100) => // good
        case f => fail(f.toString)
      }
      // and journal bookwork
      inMemoryJournal.p() match {
        case (_, p) if p == leader.data.progress => // good
        case x => fail(x.toString)
      }
      // and deletes the pending work
      assert(leader.data.acceptResponses.size == 0)
      val saveTime = inMemoryJournal.lastSaveTime.get()
      assert(saveTime > 0L && sentTime.get() > 0L && saveTime < sentTime.get())
    }
    def `rebroadcasts its commit with a fresh heartbeat when it gets prompted` {
      val sent = ArrayBuffer[Commit]()
      val io = new UndefinedIO with SilentLogging {
        override def randomTimeout: Long = 0L

        override def clock: Long = 0L

        override def send(msg: PaxosMessage): Unit = {
          msg match {
            case c: Commit =>
              sent += c
            case _ =>
          }
        }
      }
      // when it gets a heartbeat
      val leader = paxosAlgorithm(new PaxosEvent(io, agentInitialLeaderData, HeartBeat))
      // its sends out a commit
      val c1 = sent.headOption.value
      c1 match {
        case c: Commit if c.identifier == agentInitialLeaderData.data.progress.highestCommitted => // good
        case f => fail(f.toString)
      }
      // and another heartbeat
      Thread.sleep(2L)
      sent.clear()
      paxosAlgorithm(new PaxosEvent(io, leader, HeartBeat))
      // it sends out another commit
      val c2 = sent.headOption.value
      c2 match {
        case c: Commit if c.identifier == agentInitialLeaderData.data.progress.highestCommitted => // good
        case f => fail(f.toString)
      }
      // and the commits have different heartbeats
      assert(c2.heartbeat > c1.heartbeat)
    }
    def returnsToFollowerTest(messages: ArrayBuffer[PaxosMessage], sent: ArrayBuffer[PaxosMessage]): Unit = {
      // given a verifiable io
      val inMemoryJournal = new InMemoryJournal
      val sentNoLongerLeader = new AtomicBoolean()
      val io = new UndefinedIO with SilentLogging {
        override def journal: Journal = inMemoryJournal

        override def clock: Long = Long.MaxValue

        override def randomTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = sent += msg

        override def sendNoLongerLeader(clientCommands: Map[Identifier, (CommandValue, String)]): Unit =
          sentNoLongerLeader.set(true)
      }
      // and a leader who has committed slot 98 and broadcast slot 99
      val lastCommitted = Identifier(0, epoch, 98L)
      val id99 = Identifier(0, epoch, 99L)
      val a99 = Accept(id99, DummyCommandValue(99))
      val votes = TreeMap(id99 -> AcceptResponsesAndTimeout(0L, a99, Map(0 -> AcceptAck(id99, 0, initialData.progress))))
      val responses = acceptResponsesLens.set(initialData, votes)
      val committed = Progress.highestPromisedHighestCommitted.set(responses.progress, (lastCommitted.number, lastCommitted))
      val clientCommands: Map[Identifier, (CommandValue, String)] = Map(id99 -> (DummyCommandValue(99), "1"))
      val data = responses.copy(progress = committed, clientCommands = clientCommands)
      val agent = PaxosAgent(0, Leader, data, initialQuorumStrategy)
      val accept = Accept(id99, DummyCommandValue(99))
      inMemoryJournal.a.put(99L, (0L -> accept))
      // when it gets the messages
      val follower = messages.foldLeft(agent) {
        (a, m) =>
          paxosAlgorithm(new PaxosEvent(io, a, m))
      }
      // is a follower
      follower.role shouldBe Follower
      follower.data.acceptResponses.isEmpty shouldBe true
      follower.data.epoch shouldBe None
      follower.data.prepareResponses.isEmpty shouldBe true
      follower.data.clientCommands.isEmpty shouldBe true
      sentNoLongerLeader.get() shouldBe true
    }
    def `returns to follower when it receives a majority of accept nacks`{
      val sent = ArrayBuffer[PaxosMessage]()
      val messages = ArrayBuffer[PaxosMessage]()
      val id99 = Identifier(0, epoch, 99L)
      val nack1 = AcceptNack(id99, 1, initialData.progress)
      val nack2 = AcceptNack(id99, 2, initialData.progress)
      messages += nack1
      messages += nack2
      returnsToFollowerTest(messages, sent)
      sent shouldBe empty
    }
    def `returns to follower when it sees a higher commit watermark in a response` {
      val sent = ArrayBuffer[PaxosMessage]()
      val messages = ArrayBuffer[PaxosMessage]()
      val id99 = Identifier(0, epoch, 99L)
      val ack = AcceptNack(id99, 1, Progress(epoch, Identifier(0, epoch, 99L)))
      messages += ack
      returnsToFollowerTest(messages, sent)
      // it sends nothing
      sent.isEmpty shouldBe true
    }
    def `returns to follower when it makes a higher promise to another node` {
      val sent = ArrayBuffer[PaxosMessage]()
      val messages = ArrayBuffer[PaxosMessage]()
      val highIdentifier = Identifier(2, BallotNumber(Int.MaxValue, Int.MaxValue), 99L)
      val highPrepare = Prepare(highIdentifier)
      messages += highPrepare
      returnsToFollowerTest(messages, sent)
      sent.headOption.value match {
        case pa: PrepareAck => // good
        case f => fail(f.toString)
      }
    }
  }
}
