package com.github.trex_paxos.core

import com.github.trex_paxos._
import org.scalatest.{Matchers, WordSpecLike}

class MemberPickleSpec extends WordSpecLike with Matchers {

  val address1 = Address("aa", 2)
  val addresses = Addresses(address1, Address("bb", 3))

  val node1 = Node(101, addresses)
  val node2 = Node(102, Addresses(Address("cc", 4), Address("dd", 5)))
  val node3 = Node(103, Addresses(Address("ee", 6), Address("ff", 7)))

  val quorum1 = Quorum(2, Set(Weight(101, 1), Weight(102, 2)))

  val nodes = Set(node1, node2, node3)

  val membership = ClusterConfiguration(quorum1, Quorum(4, Set(Weight(101, 2), Weight(102, 2), Weight(103, 2))), nodes, Some(99L))

  "MemberPickle" should {
    "roundtrip node" in {
      val js = MemberPickle.nodeTo(node1)
      MemberPickle.nodeFrom(js) match {
        case Some(`node1`) => // good
        case f => fail(f.toString)
      }
    }
    "roundtrip quorum" in {
      val js = MemberPickle.quorumTo(quorum1)
      MemberPickle.quorumFrom(js) match {
        case Some(`quorum1`) => // good
        case f => fail(f.toString)
      }
    }
    "roundtrip membership" in {
      val js = MemberPickle.toJson(membership)
      MemberPickle.fromJson(js) match {
        case Some(`membership`) => // good
        case f => fail(f.toString)
      }
    }
    "roundtrip membership no slot" in {
      val membershipNoSlot = membership.copy(effectiveSlot = None)
      val js = MemberPickle.toJson(membershipNoSlot)
      MemberPickle.fromJson(js) match {
        case Some(`membershipNoSlot`) => // good
        case f => fail(f.toString)
      }
    }
    "roundtrip membership with era " in {
      val membershipNoSlot = membership.copy(era = Option(99))
      val js = MemberPickle.toJson(membershipNoSlot)
      MemberPickle.fromJson(js) match {
        case Some(`membershipNoSlot`) => // good
        case f => fail(f.toString)
      }
    }
  }
}