package com.github.trex_paxos.core

import com.github.trex_paxos._
import org.scalatest.{Matchers, WordSpecLike}

import scala.util.control.NonFatal

class MembershipSpec extends WordSpecLike with Matchers {
  val address = Address("localhost", 8080)
  val addresses = Addresses(address, address)
  val nodes = Set(Node(1, addresses),Node(2, addresses),Node(3, addresses))

  "Memberships " should {
    "not error when no issues" in {
      Membership(Quorum(2, Set(Weight(1,1), Weight(2,1), Weight(3,1))), Quorum(2, Set(Weight(1,1), Weight(2,1), Weight(3,1))), nodes, Some(0L))
    }
    "error if prepare and accepts quorums do not overlap" in {
      try {
        Membership(Quorum(1, Set(Weight(1,1))), Quorum(1, Set(Weight(2,1))), nodes, Some(0L))
        fail("shouldnt have gotten this far as none overlapping prepares and accepts")
      } catch {
        case e: Exception if e.getMessage.contains("quorum for promises must overlap with the quorum for accepts") => // good
        case NonFatal(f) => fail(f.toString)
      }

    }
    "error if a network location is missing" in {
      try {
        Membership(Quorum(2, Set(Weight(1,1), Weight(2,1), Weight(3,1))), Quorum(2, Set(Weight(1,1), Weight(2,1), Weight(3,1))), Set(Node(1, addresses)), Some(0L))
        fail("shouldnt have gotten this far as address missing")
      } catch {
        case e: Exception if e.getMessage.contains("The unique nodes within the combined quorums don't match the nodes for which we have network addresses") => // good
        case NonFatal(f) => fail(f.toString)
      }
    }
  }
}
