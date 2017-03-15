package com.github.trex_paxos.library

import com.github.trex_paxos.library.TestHelpers._

import scala.collection.immutable.SortedMap
import Ordering._
import org.scalamock.scalatest.MockFactory

import scala.collection.mutable.ArrayBuffer
import scala.compat.Platform

class RecovererTests extends AllRolesTests with LeaderLikeTests with MockFactory {

  val initialDataAgent = PaxosAgent(0, Recoverer, initialData, initialQuorumStrategy)

  object `The Recoverer Function` {
    def `should be defined for a recoverer and a client command message` {
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, DummyCommandValue("99"))))
    }

    def `should be defined for a recoverer and RetransmitRequest` {
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, RetransmitRequest(0, 1, 0L))))
    }

    def `should be defined for a recoverer and RetransmitResponse` {
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, RetransmitResponse(0, 1, Seq(), Seq()))))
    }

    def `should be defined for a recoverer and a Prepare if the prepare is less than the promise` {
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Prepare(Identifier(0, BallotNumber(0, 0), 0)))))
    }

    def `should be defined for a recoverer and a Prepare if the prepare is higher than the promise` {
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Prepare(Identifier(0, BallotNumber(Int.MaxValue, Int.MaxValue), 0)))))
    }

    def `should be defined for a recoverer and a Prepare if the prepare is equal to the promise` {
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Prepare(Identifier(0, initialDataAgent.data.progress.highestPromised, 0)))))
    }

    def `should be defined for a recoverer and an Accept with a lower number` {
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Accept(Identifier(0, BallotNumber(0, 0), 0), NoOperationCommandValue))))
    }

    def `should be defined for a recoverer and an Accept with a higher number for a committed slot` {
      val promise = BallotNumber(Int.MaxValue, Int.MaxValue)
      val initialData = TestHelpers.highestPromisedHighestCommittedLens.set(TestHelpers.initialData, (promise, Identifier(from = 0, number = promise, logIndex = 99L)))
      val higherCommittedAgent = PaxosAgent(0, Recoverer, initialData, initialQuorumStrategy)
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(undefinedIO, higherCommittedAgent, Accept(Identifier(0, promise, 0), NoOperationCommandValue))))
    }

    def `should be defined for an Accept equal to promise` {
      val promise = BallotNumber(Int.MaxValue, Int.MaxValue)
      val initialData = highestPromisedLens.set(TestHelpers.initialData, promise)
      val equalPromiseAgent = PaxosAgent(0, Recoverer, initialData, initialQuorumStrategy)
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(undefinedIO, equalPromiseAgent, Accept(Identifier(0, promise, 0), NoOperationCommandValue))))
    }

    def `should be defined for an Accept greater than promise` {
      val higherAcceptId = BallotNumber(Int.MaxValue, Int.MaxValue)
      val lowerPromise = BallotNumber(Int.MaxValue - 1, Int.MaxValue - 1)
      val initialData = highestPromisedLens.set(TestHelpers.initialData, lowerPromise)
      val higherPromiseAgent = PaxosAgent(0, Recoverer, initialData, initialQuorumStrategy)
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(undefinedIO, higherPromiseAgent, Accept(Identifier(0, higherAcceptId, 0), NoOperationCommandValue))))
    }

    def `should be defined for a Heartbeat` = {
      val agent = PaxosAgent(0, Recoverer, initialData, initialQuorumStrategy)
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(undefinedIO, agent, HeartBeat)))
    }

    def `should be defined for a CheckTimeout when not timed out` = {
      val agent = PaxosAgent(0, Recoverer, initialData, initialQuorumStrategy)
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(negativeClockIO, agent, CheckTimeout)))
    }

    def `should be defined for a PrepareResponse` {
      val agent = PaxosAgent(0, Recoverer, initialData, initialQuorumStrategy)
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(negativeClockIO, agent, new UndefinedPrepareResponse)))
    }

    def `should be defined for a CheckTimeout when prepares are timed out` {
      val agent = PaxosAgent(0, Recoverer, initialData.copy(prepareResponses = prepareSelfVotes), initialQuorumStrategy)
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(maxClockIO, agent, CheckTimeout)))
    }

    def `should be defined for a CheckTimeout when accepts are timed out` {
      val agent = PaxosAgent(0, Recoverer, initialData.copy(acceptResponses = emptyAcceptResponses), initialQuorumStrategy)
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(maxClockIO, agent, CheckTimeout)))
    }

    def `should deal with timed-out prepares before timed-out accepts` {
      val handleResendAcceptsInvoked =Box(false)
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
      val agent = PaxosAgent(0, Recoverer, initialData.copy(prepareResponses = prepareSelfVotes, acceptResponses = emptyAcceptResponses), initialQuorumStrategy)
      paxosAlgorithm.recovererFunction(PaxosEvent(maxClockIO, agent, CheckTimeout))

      assert(handleResendPreparesInvoked() == true && handleResendAcceptsInvoked() == false)
    }

    def `should be defined for a commit at a higher log index` {
      val agent = PaxosAgent(0, Recoverer, initialData, initialQuorumStrategy)
      val commit = Commit(Identifier(1, initialData.progress.highestPromised, Int.MaxValue))
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(maxClockIO, agent, commit)))
    }

    def `should be defined for a commit at a same log index with a higher number` {
      val agent = PaxosAgent(0, Recoverer, initialData, initialQuorumStrategy)
      val commit = Commit(Identifier(1, BallotNumber(Int.MaxValue, Int.MaxValue), initialData.progress.highestCommitted.logIndex))
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(maxClockIO, agent, commit)))
    }

    def `should be defined for a low commit` {
      val agent = PaxosAgent(0, Recoverer, initialData, initialQuorumStrategy)
      val commit = Commit(Identifier(1, BallotNumber(0, 0), Long.MinValue))
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(maxClockIO, agent, commit)))
    }

    def `should be defined for a client command message` {
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, DummyCommandValue("1"))))
    }

    def `should be defined for a RetransmitRequest` {
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, RetransmitRequest(0, 1, 0L))))
    }

    def `should be defined for a RetransmitResponse` {
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, RetransmitResponse(0, 1, Seq(), Seq()))))
    }

    def `should be defined for a Prepare if the prepare is less than the promise` {
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Prepare(Identifier(0, BallotNumber(0, 0), 0)))))
    }

    def `should be defined for a Prepare if the prepare is higher than the promise` {
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Prepare(Identifier(0, BallotNumber(Int.MaxValue, Int.MaxValue), 0)))))
    }

    def `should be defined for a Prepare if the prepare is equal to the promise` {
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Prepare(Identifier(0, initialDataAgent.data.progress.highestPromised, 0)))))
    }

    def `should be defined for an Accept with a lower number` {
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Accept(Identifier(0, BallotNumber(0, 0), 0), NoOperationCommandValue))))
    }

    def `should be defined for an Accept with a higher number for a committed slot` {
      val promise = BallotNumber(Int.MaxValue, Int.MaxValue)
      val initialData = TestHelpers.highestPromisedHighestCommittedLens.set(TestHelpers.initialData, (promise, Identifier(from = 0, number = promise, logIndex = 99L)))
      val higherCommittedAgent = PaxosAgent(0, Recoverer, initialData, initialQuorumStrategy)
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(undefinedIO, higherCommittedAgent, Accept(Identifier(0, promise, 0), NoOperationCommandValue))))
    }

    def `should be defined for a CheckTimeout when not timedout` {
      val agent = PaxosAgent(0, Follower, initialData, initialQuorumStrategy)
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(negativeClockIO, agent, CheckTimeout)))
    }
  }

  def recovererNoResponsesInClusterOfSize(numberOfNodes: Int) = {
    val prepareSelfVotes = SortedMap.empty[Identifier, Map[Int, PrepareResponse]] ++
      Seq((recoverHighPrepare.id -> Map(0 -> PrepareAck(recoverHighPrepare.id, 0, initialData.progress, 0, 0, None))))

    val data = initialData.copy(epoch = Some(recoverHighPrepare.id.number),
      prepareResponses = prepareSelfVotes, acceptResponses = SortedMap.empty)

    val agent = PaxosAgent(0, Recoverer, data, new DefaultQuorumStrategy(() => numberOfNodes))

    (agent, recoverHighPrepare.id)
  }

  object `A Recoverer` {
    def `responds is not leader` {
      respondsIsNotLeader(Recoverer)
    }
    def `should ignore a lower commit` {
      shouldIngoreLowerCommit(Recoverer)
    }
    def `should ignore a late prepare response` {
      shouldIngoreLatePrepareResponse(Leader)
    }
    def `should ingore a commit for same slot from lower numbered node` {
      shouldIngoreCommitMessageSameSlotLowerNodeId(Recoverer)
    }
    def `should backdown on commit same slot higher node number` {
      shouldBackdownOnCommitSameSlotHigherNodeId(Recoverer)
    }
    def `should backdown on higher slot commit` {
      shouldBackdownOnHigherSlotCommit(Recoverer)
    }
    def `should backdown and commit on higher slot commit` {
      shouldBackdownAndCommitOnHigherSlotCommit(Recoverer)
    }
    def `reissue same accept messages it gets a timeout and no challenge` {
      shouldReissueSameAcceptMessageIfTimeoutNoChallenge(Recoverer)
    }
    def `reissue higher accept messages upon learning of another nodes higher promise in a nack` {
      sendHigherAcceptOnLearningOtherNodeHigherPromise(Recoverer)
    }
    def `reissues higher accept message upon having made a higher promise itself by the timeout` {
      sendsHigherAcceptOnHavingMadeAHigherPromiseAtTimeout(Recoverer)
    }
    def `backs down if it has to make a higher promise` {
      val (agent, _) = recovererNoResponsesInClusterOfSize(3)
      val inMemoryJournal = new InMemoryJournal
      val sent: ArrayBuffer[PaxosMessage] = ArrayBuffer()
      val sentTime = Box(0L)
      val io = new UndefinedIO with SilentLogging {
        override def journal: Journal = inMemoryJournal

        override def clock: Long = Platform.currentTime

        override def scheduleRandomCheckTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = {
          sentTime(System.nanoTime())
          sent += msg
        }

        override def respond(results: Option[Map[Identifier, Any]]): Unit = results match {
          case None => // good
          case f => fail(f.toString)
        }
      }
      // which makes a promise to another leader
      val otherHigherPrepare = Prepare(Identifier(2, BallotNumber(lowValue + 1, 2), 1L))
      val event = new PaxosEvent(io, agent, otherHigherPrepare)
      // when we process the event
      val PaxosAgent(_, newRole, newData, _) = paxosAlgorithm(event)
      // then
      sent.headOption.value match {
        case a: PrepareAck => // good
        case f => fail(f.toString)
      }
      // and backs down to a follower as it cannot accept client values under its own epoch it cannot journal them so cannot commit so cannot lead
      newRole shouldBe Follower
    }
    def `backs down with a majority negative prepare response` {
      // given a recoverer with no responses
      val (agent, prepareId) = recovererNoResponsesInClusterOfSize(3)
      val inMemoryJournal = new InMemoryJournal
      val sent: ArrayBuffer[PaxosMessage] = ArrayBuffer()
      val sentTime = Box(0L)
      val io = new UndefinedIO with SilentLogging {
        override def journal: Journal = inMemoryJournal

        override def clock: Long = Platform.currentTime

        override def scheduleRandomCheckTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = {
          sentTime(System.nanoTime())
          sent += msg
        }

        override def respond(results: Option[Map[Identifier, Any]]): Unit = results match {
          case None => // good
          case f => fail(f.toString)
        }
      }
      val agentNack1 = paxosAlgorithm(new PaxosEvent(io, agent, PrepareNack(prepareId, 1, initialData.progress, 0, 0)))
      val PaxosAgent(_, role, data, _) =  paxosAlgorithm(new PaxosEvent(io, agentNack1, PrepareNack(prepareId, 2, initialData.progress, 0, 0)))
      // then
      role shouldBe Follower
      data.prepareResponses.isEmpty shouldBe true
      data.epoch shouldBe None
    }
    def `reboardcast prepares if it gets a timeout with no majority response` {
      // given a recoverer with no responses
      val (agent, prepareId) = recovererNoResponsesInClusterOfSize(5)
      val inMemoryJournal = new InMemoryJournal
      val sent: ArrayBuffer[PaxosMessage] = ArrayBuffer()
      val sentTime = Box(0L)
      val io = new UndefinedIO with SilentLogging {
        override def journal: Journal = inMemoryJournal

        override def clock: Long = Long.MaxValue

        override def scheduleRandomCheckTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = {
          sentTime(System.nanoTime())
          sent += msg
        }
      }

      // when a minority prepare response with an ack from node1
      val ack1 = PrepareAck(prepareId, 1, initialData.progress, 0, 0, None)

      // ack
      val eventAck = new PaxosEvent(io, agent, ack1)
      val agentAck = paxosAlgorithm(eventAck)
      // timeout
      val eventTimeout = new PaxosEvent(io, agentAck, CheckTimeout)
      paxosAlgorithm(eventTimeout)

      // then it reboardcasts the accept message
      sent.headOption.value shouldBe recoverHighPrepare
    }
    def `requests retransmission if is behind when gets a majority showing others have higher commit watermark` {
      // given a recoverer with self vote
      val (agent, prepareId) = recovererNoResponsesInClusterOfSize(3)
      val inMemoryJournal = new InMemoryJournal
      val sent: ArrayBuffer[PaxosMessage] = ArrayBuffer()
      val sentTime = Box(0L)
      val io = new UndefinedIO with SilentLogging {
        override def journal: Journal = inMemoryJournal

        override def clock: Long = Long.MaxValue

        override def scheduleRandomCheckTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = {
          sentTime(System.nanoTime())
          sent += msg
        }
      }
      // when a majority prepare response with an ack from node1 which shows it is behind
      val higherProgress = initialData.progress.copy(highestCommitted =
        initialData.progress.highestCommitted.copy(logIndex = 5L))
      val ack1 = PrepareAck(prepareId, 1, higherProgress, 0, 0, None)
      val eventAck = new PaxosEvent(io, agent, ack1)
      paxosAlgorithm(eventAck)
      sent.headOption.value match {
        case RetransmitRequest(0, 1, 0L) => // good
        case f => fail(f.toString)
      }
      sent.lastOption.value match {
        case Accept(prepareId, NoOperationCommandValue) => // good
        case f => fail(f.toString)
      }
    }
    def `issue new prepares if it learns from the majority that other nodes have higher accepted values` {
      // given a recoverer with self vote
      val (agent, prepareId) = recovererNoResponsesInClusterOfSize(3)
      // and verifiable io
      val inMemoryJournal = new InMemoryJournal
      val sent: ArrayBuffer[PaxosMessage] = ArrayBuffer()
      val sentTime = Box(0L)
      val io = new UndefinedIO with SilentLogging {
        override def journal: Journal = inMemoryJournal

        override def clock: Long = Long.MaxValue

        override def scheduleRandomCheckTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = {
          sentTime(System.nanoTime())
          sent += msg
        }
      }
      // when a majority prepare response with an ack from node1 which shows it has missed values
      val ack1 = PrepareAck(prepareId, 1, initialData.progress, 3, 0, None)
      val PaxosAgent(_, role, data, _) = paxosAlgorithm(new PaxosEvent(io, agent, ack1))
      // then it sends three messages
      sent.size shouldBe 3
      // recover prepare for slot 2
      val identifier2 = prepareId.copy(logIndex = 2L)
      val prepare2 = Prepare(identifier2)
      sent(0) shouldBe prepare2
      // recover prepare for slot 3
      val identifier3 = prepareId.copy(logIndex = 3L)
      val prepare3 = Prepare(identifier3)
      sent(1) shouldBe prepare3
      // and sends the accept for the majority response
      sent(2) shouldBe Accept(prepareId, NoOperationCommandValue)
      // it says as recover
      role shouldBe Recoverer
      // and makes self promises to the new prepare
      data.prepareResponses match {
        case map if map.isEmpty =>
          fail("map.isEmpty")
        case map =>
          assert(map.size == 2)
          val keys = map.keys.toSeq
          assert(keys.contains(identifier2))
          assert(keys.contains(identifier3))
      }
      // and accepts its own values
      data.acceptResponses match {
        case accepts if accepts.isEmpty =>
          fail("accepts.isEmpty")
        case accepts =>
          val keys = accepts.keys.toSeq
          assert(keys.contains(prepareId))
      }
      // and send happens after save
      val saveTime = inMemoryJournal.lastSaveTime.get()
      assert(saveTime > 0L && sentTime() > 0L && saveTime < sentTime())
    }
    def `promote to Leader and ack its own accept if it has not made a higher promise` {
      // given a recoverer with self vote
      val (agent, prepareId) = recovererNoResponsesInClusterOfSize(3)
      // and verifiable io
      val inMemoryJournal = new InMemoryJournal
      val sent: ArrayBuffer[PaxosMessage] = ArrayBuffer()
      val sentTime = Box(0L)
      val io = new UndefinedIO with SilentLogging {
        override def journal: Journal = inMemoryJournal

        override def clock: Long = Long.MaxValue

        override def scheduleRandomCheckTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = {
          sentTime(System.nanoTime())
          sent += msg
        }
      }

      // when a majority prepare response with an ack from node1
      val ack1 = PrepareAck(prepareId, 1, initialData.progress, 0, 0, None)
      val PaxosAgent(_, role, data, _) = paxosAlgorithm(new PaxosEvent(io, agent, ack1))

      // then it boardcasts a no-op
      val accept = Accept(prepareId, NoOperationCommandValue)
      sent.headOption.value match {
        case `accept` => // good
        case f => fail(f.toString)
      }
      // and becomes leader
      role shouldBe Leader
      // and has acked its own accept
      data.acceptResponses match {
        case map if map.isEmpty =>
          fail("map.isEmpty")
        case map =>
          map.get(accept.id) match {
            case None =>
              fail("None")
            case Some(AcceptResponsesAndTimeout(_, _, responses)) =>
              responses.values.headOption match {
                case Some(a: AcceptAck) => // good
                case x =>
                  fail(x.toString)
              }
          }
      }
      // and set its epoch
      data.epoch shouldBe Some(prepareId.number)
    }
  }

  /**
   * The following are more like integration tests that pass through recover into leader
   */
  object `Full recovery with commit` {
    def `fix a no-op and promote to Leader then commits if in a three node cluster gets a majority with one ack with no values to fix` {
      // given a recoverer with self vote
      val (agent, prepareId) = recovererNoResponsesInClusterOfSize(3)
      // and verifiable io
      val inMemoryJournal = new InMemoryJournal
      val sent: ArrayBuffer[PaxosMessage] = ArrayBuffer()
      val sentTime = Box(0L)
      val io = new UndefinedIO with SilentLogging {
        override def journal: Journal = inMemoryJournal

        override def clock: Long = Long.MaxValue

        override def scheduleRandomCheckTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = {
          sentTime(System.nanoTime())
          sent += msg
        }

        override def deliver(payload: Payload): Any = {}

        override def respond(results: Option[Map[Identifier, Any]]): Unit = results match {
          case None => fail("None")
          case _ => // good
        }
      }
      // when a majority prepare response with an ack from node1
      val prepareAck = PrepareAck(prepareId, 1, initialData.progress, 0, 0, None)
      val leader@PaxosAgent(_, role, _, _)= paxosAlgorithm(new PaxosEvent(io, agent, prepareAck))
      // then it boardcasts a no-op
      val accept = Accept(prepareId, NoOperationCommandValue)
      sent.headOption.value match {
        case `accept` => // good
        case f => fail(f.toString)
      }
      // and becomes leader
      role shouldBe Leader
      // when it gets a majority accept response with an ack from node1
      sent.clear()
      val acceptAck = AcceptAck(prepareId, 1, initialData.progress)
      val PaxosAgent(_, _, data, _) = paxosAlgorithm(new PaxosEvent(io, leader, acceptAck))
      // it commits the no-op
      sent.headOption.value match {
        case Commit(prepareId, _) => // good
        case f => fail(f.toString)
      }
      // and it has the epoch
      data.epoch shouldBe Some(prepareId.number)
      // and it has cleared the recover votes
      data.prepareResponses.isEmpty shouldBe true
      // and sets a fresh timeout
      data.timeout shouldBe 1234L
      // and send happens after save
      val saveTime = inMemoryJournal.lastSaveTime.get()
      assert(saveTime > 0L && sentTime() > 0L && saveTime < sentTime())
    }
    def `fix a no-op and promote to Leader then commits if in a three node cluster gets a majority with one ack` {
      // given a recoverer with self vote
      val (agent, prepareId) = recovererNoResponsesInClusterOfSize(3)
      // and verifiable io
      val lastDelivered: Box[CommandValue] = new Box(None)
      val inMemoryJournal = new InMemoryJournal
      val sent: ArrayBuffer[PaxosMessage] = ArrayBuffer()
      val sentTime = Box(0L)
      val cmd = DummyCommandValue("0")
      val io = new UndefinedIO with SilentLogging {
        override def journal: Journal = inMemoryJournal

        override def clock: Long = Long.MaxValue

        override def scheduleRandomCheckTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = {
          sentTime(System.nanoTime())
          sent += msg
        }

        override def deliver(payload: Payload): Any = {
          lastDelivered(payload.command)
          value
        }

        override def respond(results: Option[Map[Identifier, Any]]): Unit = results match {
          case None => fail("should have committed some results")
          case _ => // good
        }
      }
      // when a majority prepare response with an ack from node1
      // and some value returned in the promise from node1 with some lower number
      val lowerId = prepareId.copy(number = prepareId.number.copy(counter = prepareId.number.counter - 1))
      val prepareAck = PrepareAck(prepareId, 1, initialData.progress, 0, 0, Some(Accept(lowerId, cmd)))
      val leader@PaxosAgent(_, role, _, _)= paxosAlgorithm(new PaxosEvent(io, agent, prepareAck))
      // then it boardcasts the payload from the promise under its higher epoch number
      val accept = Accept(prepareId, DummyCommandValue("0"))
      sent.headOption.value match {
        case `accept` => // good
        case f => fail(f.toString)
      }
      // and becomes leader
      role shouldBe Leader
      // when it gets a majority accept response with an ack from node1
      sent.clear()
      val acceptAck = AcceptAck(prepareId, 1, initialData.progress)
      val PaxosAgent(_, _, data, _) = paxosAlgorithm(new PaxosEvent(io, leader, acceptAck))
      // it delivers the value
      Option(lastDelivered()) match {
        case Some(DummyCommandValue("0")) => // good
        case f => fail(f.toString)
      }
      // and sends the commit
      sent.headOption.value match {
        case Commit(prepareId, _) => // good
        case f => fail(f.toString)
      }
      // and it has the epoch
      data.epoch shouldBe Some(prepareId.number)
      // and it has cleared the recover votes
      data.prepareResponses.isEmpty shouldBe true
      // and sets a fresh timeout
      data.timeout shouldBe 1234L
      // and send happens after save
      val saveTime = inMemoryJournal.lastSaveTime.get()
      assert(saveTime > 0L && sentTime() > 0L && saveTime < sentTime())
    }
    def `fix a no-op promote to Leader and commits if in a five node cluster gets a majority with two acks with no values to fix` {
      // given a recoverer with self vote
      val (agent, prepareId) = recovererNoResponsesInClusterOfSize(5)
      // and verifiable io
      val inMemoryJournal = new InMemoryJournal
      val sent: ArrayBuffer[PaxosMessage] = ArrayBuffer()
      val sentTime = Box(0L)
      val io = new UndefinedIO with SilentLogging {
        override def journal: Journal = inMemoryJournal

        override def clock: Long = Long.MaxValue

        override def scheduleRandomCheckTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = {
          sentTime(System.nanoTime())
          sent += msg
        }

        override def deliver(payload: Payload): Any = {}

        override def respond(results: Option[Map[Identifier, Any]]): Unit = results match {
          case f@None => fail(f.toString)
          case _ => // good
        }
      }
      // when a majority prepare response with an ack from node1 and node2
      val ack1 = PrepareAck(prepareId, 1, initialData.progress, 0, 0, None)
      val ack1Agent@PaxosAgent(_, roleAck1, _, _) = paxosAlgorithm(new PaxosEvent(io, agent, ack1))
      roleAck1 shouldBe Recoverer
      val ack2 = PrepareAck(prepareId, 2, initialData.progress, 0, 0, None)
      val leader@PaxosAgent(_, role, _, _) = paxosAlgorithm(new PaxosEvent(io, ack1Agent, ack2))
      // then
      role shouldBe Leader
      // then it boardcasts a no-op
      val accept = Accept(prepareId, NoOperationCommandValue)
      sent.headOption.value match {
        case `accept` => // good
        case f => fail(f.toString)
      }
      // when it gets a majority accept response with an ack from node1
      sent.clear()
      val acceptAck1 = AcceptAck(prepareId, 1, initialData.progress)
      val leader2 = paxosAlgorithm(new PaxosEvent(io, leader, acceptAck1))
      sent.isEmpty shouldBe true
      val acceptAck2 = AcceptAck(prepareId, 2, initialData.progress)
      val PaxosAgent(_, _, data, _) = paxosAlgorithm(new PaxosEvent(io, leader2, acceptAck2))
      // it commits the no-op
      sent.headOption.value match {
        case Commit(prepareId, _) => // good
        case f => fail(f.toString)
      }
      // and it has the epoch
      data.epoch shouldBe Some(prepareId.number)
      // and it has cleared the recover votes
      data.prepareResponses.isEmpty shouldBe true
      // and sets a fresh timeout
      data.timeout shouldBe 1234L
      // and send happens after save
      val saveTime = inMemoryJournal.lastSaveTime.get()
      assert(saveTime > 0L && sentTime() > 0L && saveTime < sentTime())
    }
    def `fix a no-op and promote to Leader then commits if in a five node cluster gets a majority with two acks` {
      // given a recoverer with self vote
      val (agent, prepareId) = recovererNoResponsesInClusterOfSize(5)
      // and verifiable io
      val lastDelivered: Box[CommandValue] = new Box(None)
      val inMemoryJournal = new InMemoryJournal
      val sent: ArrayBuffer[PaxosMessage] = ArrayBuffer()
      val sentTime = Box(0L)
      val io = new UndefinedIO with SilentLogging {
        override def journal: Journal = inMemoryJournal

        override def clock: Long = Long.MaxValue

        override def scheduleRandomCheckTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = {
          sentTime(System.nanoTime())
          sent += msg
        }

        override def deliver(payload: Payload): Any = {
          lastDelivered(payload.command)
          value
        }

        override def respond(results: Option[Map[Identifier, Any]]): Unit = {}
      }
      // when a majority prepare response with an ack from node1
      // and some value returned in the promise from node1 with some lower number
      val lowerId = prepareId.copy(number = prepareId.number.copy(counter = prepareId.number.counter - 1))
      val prepareAck1 = PrepareAck(prepareId, 1, initialData.progress, 0, 0, Some(Accept(lowerId, DummyCommandValue("0"))))
      val ack1 = paxosAlgorithm(new PaxosEvent(io, agent, prepareAck1))
      val prepareAck2 = PrepareAck(prepareId, 2, initialData.progress, 0, 0, Some(Accept(lowerId, DummyCommandValue("0"))))
      val leader@PaxosAgent(_, roleLeader, _, _) = paxosAlgorithm(new PaxosEvent(io, ack1, prepareAck2))

      // then it boardcasts the payload from the promise under its higher epoch number
      val accept = Accept(prepareId, DummyCommandValue("0"))
      sent.headOption.value match {
        case `accept` => // good
        case f => fail(f.toString)
      }
      // and becomes leader
      roleLeader shouldBe Leader
      // when it gets majority accept responses
      sent.clear()
      val acceptAck1 = AcceptAck(prepareId, 1, initialData.progress)
      val acceptAck1agent = paxosAlgorithm(new PaxosEvent(io, leader, acceptAck1))
      val acceptAck2 = AcceptAck(prepareId, 2, initialData.progress)
      val PaxosAgent(_, _, data, _) = paxosAlgorithm(new PaxosEvent(io, acceptAck1agent, acceptAck2))
      // it delivers the value
      Option(lastDelivered()) match {
        case Some(DummyCommandValue("0")) => // good
        case f => fail(f.toString)
      }
      // and sends the commit
      sent.headOption.value match {
        case Commit(prepareId, _) => // good
        case f => fail(f.toString)
      }
      // and it has the epoch
      data.epoch shouldBe Some(prepareId.number)
      // and it has cleared the recover votes
      data.prepareResponses.isEmpty shouldBe true
      // and sets a fresh timeout
      data.timeout shouldBe 1234L
      // and send happens after save
      val saveTime = inMemoryJournal.lastSaveTime.get()
      assert(saveTime > 0L && sentTime() > 0L && saveTime < sentTime())
    }
  }
}
