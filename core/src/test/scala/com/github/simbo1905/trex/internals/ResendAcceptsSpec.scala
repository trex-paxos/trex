package com.github.simbo1905.trex.internals

import org.scalatest.{Matchers, WordSpecLike}

import scala.collection.SortedMap
import scala.collection.immutable.TreeMap

class ResendAcceptsSpec extends WordSpecLike with Matchers {
  import ResendAcceptsSpec._
  "ResendAcceptsHandler" should {
    "find the timed-out accepts in" in {
      val timedout = ResendAcceptsHandler.timedout(100L, emptyAcceptResponses)
      timedout shouldBe timedOutAt100AcceptResponses
    }
    "detect highest own promise" in {
      val highest = ResendAcceptsHandler.highestPromise(BallotNumber(4, 4), emptyAcceptResponses)
      highest shouldBe BallotNumber(4, 4)
    }
    "detect others highest promise" in {
      val highPromise = emptyAcceptResponses +
        (a99.id -> AcceptResponsesAndTimeout(50L, a99, Map(0 -> AcceptNack(a99.id, 0, progressWith(BallotNumber(99, 99), zeroProgress.highestPromised)))))
      val highest = ResendAcceptsHandler.highestPromise(BallotNumber(1, 1), highPromise)
      highest shouldBe BallotNumber(99, 99)
    }
    "detect others highest committed" in {
      val highPromise = emptyAcceptResponses +
        (a99.id -> AcceptResponsesAndTimeout(50L, a99, Map(0 -> AcceptNack(a99.id, 0, progressWith(zeroProgress.highestPromised, BallotNumber(99, 99))))))
      val highest = ResendAcceptsHandler.highestPromise(BallotNumber(1, 1), highPromise)
      highest shouldBe BallotNumber(99, 99)
    }
    "refresh accepts" in {
      val newNumber = BallotNumber(4, 4)
      val refreshed = ResendAcceptsHandler.refreshAccepts(newNumber, Seq(a98, a99, a100))
      val identifier98: Identifier = Identifier(1, newNumber, 98L)
      val identifier99: Identifier = Identifier(2, newNumber, 99L)
      val identifier100: Identifier = Identifier(3, newNumber, 100L)
      refreshed shouldBe Seq(a98.copy(id = identifier98), a99.copy(id = identifier99), a100.copy(id = identifier100))
    }
    "sets a new timeout on resend accept" in {
      //fail() FIXME
    }
    "sets a new timeout per refreshed accept" in {
      //fail() FIXME
    }
  }
}

object ResendAcceptsSpec {
  import Ordering._
  val identifier98: Identifier = Identifier(1, BallotNumber(1, 1), 98L)
  val identifier99: Identifier = Identifier(2, BallotNumber(2, 2), 99L)
  val identifier100: Identifier = Identifier(3, BallotNumber(3, 3), 100L)

  val a98 = Accept(identifier98, NoOperationCommandValue)
  val a99 = Accept(identifier99, NoOperationCommandValue)
  val a100 = Accept(identifier100, NoOperationCommandValue)

  val emptyAcceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout] = TreeMap(
    a98.id -> AcceptResponsesAndTimeout(100L, a98, Map.empty),
    a99.id -> AcceptResponsesAndTimeout(50L, a99, Map.empty),
    a100.id -> AcceptResponsesAndTimeout(120L, a100, Map.empty)
  )

  val timedOutAt100AcceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout] = TreeMap(
    a98.id -> AcceptResponsesAndTimeout(100L, a98, Map.empty),
    a100.id -> AcceptResponsesAndTimeout(120L, a100, Map.empty)
  )

  val zeroProgress = Progress(BallotNumber(0,0), Identifier(0, BallotNumber(0,0), 0L))

  def progressWith(promise: BallotNumber, committed: BallotNumber) = zeroProgress.copy(highestPromised = promise,
    highestCommitted = zeroProgress.highestCommitted.copy(number = committed))

}
