package com.github.trex_paxos.library

import TestHelpers._

import scala.collection.immutable.{TreeMap, SortedMap}

import Ordering._

class RecovererTests extends AllRolesTests with LeaderLikeTests {

  val initialDataAgent = PaxosAgent(0, Recoverer, initialData)

  object `The Recoverer Function` {
    def `should be defined for a recoverer and a client command message` {
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, ClientRequestCommandValue(0L, Array.empty[Byte]))))
    }

    def `should be defined for a recoverer and RetransmitRequest` {
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, RetransmitRequest(0, 1, 0L))))
    }

    def `should be defined for a recoverer and RetransmitResponse` {
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, RetransmitResponse(0, 1, Seq(), Seq()))))
    }

    def `should be defined for a recoverer and a Prepare if the prepare is less than the promise` {
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Prepare(Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), 0)))))
    }

    def `should be defined for a recoverer and a Prepare if the prepare is higher than the promise` {
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Prepare(Identifier(0, BallotNumber(Int.MaxValue, Int.MaxValue), 0)))))
    }

    def `should be defined for a recoverer and a Prepare if the prepare is equal to the promise` {
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Prepare(Identifier(0, initialDataAgent.data.progress.highestPromised, 0)))))
    }

    def `should be defined for a recoverer and an Accept with a lower number` {
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Accept(Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), 0), NoOperationCommandValue))))
    }

    def `should be defined for a recoverer and an Accept with a higher number for a committed slot` {
      val promise = BallotNumber(Int.MaxValue, Int.MaxValue)
      val initialData = TestHelpers.highestPromisedHighestCommittedLens.set(TestHelpers.initialData, (promise, Identifier(from = 0, number = promise, logIndex = 99L)))
      val higherCommittedAgent = PaxosAgent(0, Recoverer, initialData)
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(undefinedIO, higherCommittedAgent, Accept(Identifier(0, promise, 0), NoOperationCommandValue))))
    }

    def `should be defined for an Accept equal to promise` {
      val promise = BallotNumber(Int.MaxValue, Int.MaxValue)
      val initialData = highestPromisedLens.set(TestHelpers.initialData, promise)
      val equalPromiseAgent = PaxosAgent(0, Recoverer, initialData)
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(undefinedIO, equalPromiseAgent, Accept(Identifier(0, promise, 0), NoOperationCommandValue))))
    }

    def `should be defined for an Accept greater than promise` {
      val higherAcceptId = BallotNumber(Int.MaxValue, Int.MaxValue)
      val lowerPromise = BallotNumber(Int.MaxValue - 1, Int.MaxValue - 1)
      val initialData = highestPromisedLens.set(TestHelpers.initialData, lowerPromise)
      val higherPromiseAgent = PaxosAgent(0, Recoverer, initialData)
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(undefinedIO, higherPromiseAgent, Accept(Identifier(0, higherAcceptId, 0), NoOperationCommandValue))))
    }

    def `should be defined for a Heartbeat` = {
      val agent = PaxosAgent(0, Recoverer, initialData)
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(undefinedIO, agent, HeartBeat)))
    }

    def `should be defined for a CheckTimeout when not timed out` = {
      val agent = PaxosAgent(0, Recoverer, initialData)
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(negativeClockIO, agent, CheckTimeout)))
    }

    def `should be defined for a PrepareResponse` {
      val agent = PaxosAgent(0, Recoverer, initialData)
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(negativeClockIO, agent, new UndefinedPrepareResponse)))
    }

    def `should be defined for a CheckTimeout when prepares are timed out` {
      val agent = PaxosAgent(0, Recoverer, initialData.copy(prepareResponses = prepareSelfVotes))
      assert(paxosAlgorithm.recovererFunction.isDefinedAt(PaxosEvent(maxClockIO, agent, CheckTimeout)))
    }

    def `should be defined for a CheckTimeout when accepts are timed out` {
      val agent = PaxosAgent(0, Recoverer, initialData.copy(acceptResponses = emptyAcceptResponses))
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(maxClockIO, agent, CheckTimeout)))
    }

    def `should deal with timed-out prepares before timed-out accepts` {
      var handleResendAcceptsInvoked = false
      var handleResendPreparesInvoked = false
      val paxosAlgorithm = new PaxosAlgorithm {
        override def handleResendAccepts(io: PaxosIO, agent: PaxosAgent, time: Long): PaxosAgent = {
          handleResendAcceptsInvoked = true
          agent
        }
        override def handleResendPrepares(io: PaxosIO, agent: PaxosAgent, time: Long): PaxosAgent = {
          handleResendPreparesInvoked = true
          agent
        }
      }
      val agent = PaxosAgent(0, Recoverer, initialData.copy(prepareResponses = prepareSelfVotes, acceptResponses = emptyAcceptResponses))
      paxosAlgorithm.recovererFunction(PaxosEvent(maxClockIO, agent, CheckTimeout))
      assert(handleResendPreparesInvoked == true && handleResendAcceptsInvoked == false)
    }

    def `should be defined for a commit at a higher log index` {
      val agent = PaxosAgent(0, Recoverer, initialData)
      val commit = Commit(Identifier(1, initialData.progress.highestPromised, Int.MaxValue))
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(maxClockIO, agent, commit)))
    }

    def `should be defined for a commit at a same log index with a higher number` {
      val agent = PaxosAgent(0, Recoverer, initialData)
      val commit = Commit(Identifier(1, BallotNumber(Int.MaxValue, Int.MaxValue), initialData.progress.highestCommitted.logIndex))
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(maxClockIO, agent, commit)))
    }

    def `should be defined for a low commit` {
      val agent = PaxosAgent(0, Recoverer, initialData)
      val commit = Commit(Identifier(1, BallotNumber(Int.MinValue, Int.MinValue), Long.MinValue))
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(maxClockIO, agent, commit)))
    }

    def `should be defined for a client command message` {
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, ClientRequestCommandValue(0L, Array.empty[Byte]))))
    }

    def `should be defined for a RetransmitRequest` {
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, RetransmitRequest(0, 1, 0L))))
    }

    def `should be defined for a RetransmitResponse` {
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, RetransmitResponse(0, 1, Seq(), Seq()))))
    }

    def `should be defined for a Prepare if the prepare is less than the promise` {
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Prepare(Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), 0)))))
    }

    def `should be defined for a Prepare if the prepare is higher than the promise` {
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Prepare(Identifier(0, BallotNumber(Int.MaxValue, Int.MaxValue), 0)))))
    }

    def `should be defined for a Prepare if the prepare is equal to the promise` {
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Prepare(Identifier(0, initialDataAgent.data.progress.highestPromised, 0)))))
    }

    def `should be defined for an Accept with a lower number` {
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, Accept(Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), 0), NoOperationCommandValue))))
    }

    def `should be defined for an Accept with a higher number for a committed slot` {
      val promise = BallotNumber(Int.MaxValue, Int.MaxValue)
      val initialData = TestHelpers.highestPromisedHighestCommittedLens.set(TestHelpers.initialData, (promise, Identifier(from = 0, number = promise, logIndex = 99L)))
      val higherCommittedAgent = PaxosAgent(0, Recoverer, initialData)
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(undefinedIO, higherCommittedAgent, Accept(Identifier(0, promise, 0), NoOperationCommandValue))))
    }

    def `should be defined for a CheckTimeout when not timedout` {
      val agent = PaxosAgent(0, Follower, initialData)
      assert(paxosAlgorithm.recoveringFunction.isDefinedAt(PaxosEvent(negativeClockIO, agent, CheckTimeout)))
    }
  }

  object `A Recoverer` {
    def `should use prepare handler` {
      usesPrepareHandler(Recoverer)
    }
    def `responds is not leader` {
      respondsIsNotLeader(Recoverer)
    }
    def `should ignore a lower commit` {
      shouldIngoreLowerCommit(Recoverer)
    }
    def `should ingore a commit for same slot from lower numbered node` {
      shouldIngoreCommitMessageSameSlotLowerNodeId(Recoverer)
    }
    def `should backdown on commit same slot higher node number` {
      shouldBackdownOnCommitSameSlotHigherNodeId(Recoverer)
    }
  }

}
