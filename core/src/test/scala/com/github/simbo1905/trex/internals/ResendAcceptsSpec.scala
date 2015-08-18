package com.github.simbo1905.trex.internals

import org.scalatest.{Matchers, WordSpecLike}

import scala.collection.SortedMap
import scala.collection.immutable.TreeMap

class ResendAcceptsSpec extends WordSpecLike with Matchers {
  import ResendAcceptsSpec._
  "ResendAcceptsHandler" should {
    "find the timed-out accepts in" in {
      val timedout = ResendAcceptsHandler.timedout(100L, acceptResponses)
      timedout shouldBe Seq(a98, a100)
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

  val acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout] = TreeMap(
    a98.id -> AcceptResponsesAndTimeout(100L, a98, Map.empty),
    a99.id -> AcceptResponsesAndTimeout(50L, a99, Map.empty),
    a100.id -> AcceptResponsesAndTimeout(120L, a100, Map.empty)
  )
}
