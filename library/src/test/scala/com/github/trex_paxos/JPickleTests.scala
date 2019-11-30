package com.github.trex_paxos

import java.util.Arrays.{equals => bequals}

import com.github.trex_paxos.library._
import org.scalatest.{Matchers, WordSpecLike}

class JPickleTests extends WordSpecLike with Matchers {
  "Java Pickle" should {

    "round-trip Progress" in {
      val p = Journal.minBookwork;
      JPickle.unpickleProgress(JPickle.pickleProgress(p)) match {
        case `p` =>
        case f => fail(f.toString)
      }
    }

    val bytes1 = Array[Byte](5, 6)

    "round-trip Accept" in {
      {
        val a = Accept(Identifier(1, BallotNumber(2, 3), 4L), ClientCommandValue("0", bytes1))
        val b = JPickle.pickleAccept(a);
        JPickle.unpickleAccept(b) match {
          case Accept(Identifier(1, BallotNumber(2, 3), 4L), ClientCommandValue("0", bout)) =>
            assert(bequals(Array[Byte](5, 6), bout))
          case f => fail(f.toString)
        }
      }
      {
        val a = Accept(Identifier(1, BallotNumber(2, 3), 4L), NoOperationCommandValue)
        val b = JPickle.pickleAccept(a);
        JPickle.unpickleAccept(b) match {
          case `a` =>
          case f => fail(f.toString)
        }
      }
    }
  }
}
