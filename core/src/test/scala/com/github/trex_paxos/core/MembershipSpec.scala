package com.github.trex_paxos.core

import com.github.trex_paxos.{Membership, Quorum}
import org.scalatest.{Matchers, WordSpecLike}

import scala.util.control.NonFatal

class MembershipSpec extends WordSpecLike with Matchers {
    "Memberships " should {
      "not error when no issues" in {
        Membership(0L, Quorum(2, Set(1,2,3)), Quorum(2, Set(1,2,3)), Map(1 -> "", 2 -> "", 3 -> ""))
      }
      "error if prepare and accepts quorums do not overlap" in {
        try {
          Membership(0L, Quorum(1, Set(1)), Quorum(1, Set(2)), Map(1 -> "", 2 -> ""))
          fail("shouldnt have gotten this far as none overlapping prepares and accepts")
         } catch {
          case e: Exception if e.getMessage.contains("quorum for promises must overlap with the quorum for accepts") => // good
          case NonFatal(f) => fail(f.toString)
        }

      }
      "error if a network location is missing" in {
        try {
          Membership(0L, Quorum(2, Set(1,2,3)), Quorum(2, Set(1,2,3)), Map(1 -> "", 2 -> "", 4 -> ""))
          fail("shouldnt have gotten this far as none overlapping prepares and accepts")
        } catch {
          case e: Exception if e.getMessage.contains("Network locations are required for all nodes") => // good
          case NonFatal(f) => fail(f.toString)
        }
      }
    }
}
