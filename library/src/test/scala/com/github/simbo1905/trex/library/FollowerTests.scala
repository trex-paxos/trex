package com.github.simbo1905.trex.library

import TestHelpers._
import Ordering._

import scala.collection.immutable.TreeMap

class FollowerTests extends AllTests {

  class ClockPaxosIO(time: Long) extends UndefinedIO {
    override def clock: Long = time
  }

  val initialDataAgent = PaxosAgent(0, Follower, initialData)

  object `The FollowingFunction` {

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
    def `should be defined for a client command message` {
      assert(paxosAlgorithm.followerFunction.isDefinedAt(PaxosEvent(undefinedIO, initialDataAgent, ClientRequestCommandValue(0L, Array.empty[Byte]))))
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
      val higherCommittedAgent = PaxosAgent(0, Follower, initialData)
      assert(paxosAlgorithm.followerFunction.isDefinedAt(PaxosEvent(undefinedIO, higherCommittedAgent, Accept(Identifier(0, promise, 0), NoOperationCommandValue))))
    }

    def `should be defined for an Accept equal to promise` {
      val promise = BallotNumber(Int.MaxValue, Int.MaxValue)
      val initialData = highestPromisedLens.set(TestHelpers.initialData, promise)
      val equalPromiseAgent = PaxosAgent(0, Follower, initialData)
      assert(paxosAlgorithm.followerFunction.isDefinedAt(PaxosEvent(undefinedIO, equalPromiseAgent, Accept(Identifier(0, promise, 0), NoOperationCommandValue))))
    }

    def `should be defined for an Accept greater than promise` {
      val higherAcceptId = BallotNumber(Int.MaxValue, Int.MaxValue)
      val lowerPromise = BallotNumber(Int.MaxValue -1 , Int.MaxValue - 1)
      val initialData = highestPromisedLens.set(TestHelpers.initialData, lowerPromise)
      val higherPromiseAgent = PaxosAgent(0, Follower, initialData)
      assert(paxosAlgorithm.followerFunction.isDefinedAt(PaxosEvent(undefinedIO, higherPromiseAgent, Accept(Identifier(0, higherAcceptId, 0), NoOperationCommandValue))))
    }

    def `should be defined for a Heartbeat` = {
      val agent = PaxosAgent(0, Follower, initialData)
      assert(paxosAlgorithm.followerFunction.isDefinedAt(PaxosEvent(undefinedIO, agent, HeartBeat)))
    }

    def `should be defined for a CheckTimeout when not timedout` {
      val agent = PaxosAgent(0, Follower, initialData)
      assert(paxosAlgorithm.followerFunction.isDefinedAt(PaxosEvent(negativeClockIO, agent, CheckTimeout)))
    }

    def `should nack a low prepare` {
      nackLowPrepare(Follower)
    }

  }

}
