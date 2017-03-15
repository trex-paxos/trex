package com.github.trex_paxos.library

import org.scalamock.scalatest.MockFactory
import org.scalatest.refspec.RefSpec
import org.scalatest.{Matchers, OptionValues}

/**
  * The stunning result in FPaxos is that accept quorums dont have to overlap
  * with one another they only need to overlap with the promise quorum. This
  * is written as:
  * |P| + |Q| > N
  * which says "the size of the promise quorum
  * plus the size of the accept quorum must be greater than the number of
  * nodes.
  */
class QuorumStrategyTests extends RefSpec with Matchers with MockFactory with OptionValues {
  import TestHelpers._
  object `The DefaultQuorumStrategy` {
    def `should see a quorum of 1 in a cluster size of 1`: Unit = {
      val cluster1 = new DefaultQuorumStrategy(()=>1)
      cluster1.promiseQuorumSize shouldBe 1
      cluster1.assessAccepts(Seq(a98ack0)) shouldBe Option(QuorumAck)
      cluster1.assessAccepts(Seq(a98nack1)) shouldBe Option(QuorumNack)
      cluster1.assessPromises(Seq(pAck)) shouldBe Option(QuorumAck)
      cluster1.assessPromises(Seq(pNack)) shouldBe Option(QuorumNack)
    }
    /**
      * 2 + 1 > 2 so proven to be safe in the FPaxos paper.
      * of course you have no resilience with two nodes but perhaps someone
      * might need to do some emergency cluster maintainance where they need to
      * run on two nodes for a short period.
      */
    def `should see an accept quorum of 1 in a cluster size of 2`: Unit = {
      val cluster1 = new DefaultQuorumStrategy(()=>2)
      cluster1.promiseQuorumSize shouldBe 2

      cluster1.assessPromises(Seq(pAck)) shouldBe None
      cluster1.assessPromises(Seq(pNack)) shouldBe None

      cluster1.assessAccepts(Seq(a98ack0)) shouldBe Option(QuorumAck) // FPaxos!
      cluster1.assessAccepts(Seq(a98nack1)) shouldBe Option(QuorumNack) // FPaxos!

      cluster1.assessPromises(Seq(pAck, pNack)) shouldBe Option(SplitVote)

      cluster1.assessAccepts(Seq(a98ack0, a98nack1)).value match {
        case _: Outcome =>
          // If we are running FPaxos its an impossible result to see both an ack and a nack
          // in a two node cluster. The leader should have seen its own self accepted ack
          // as quorum and should not have waited to see what the other node said which cannot
          // have promoted to be a leader to send an nack. So whatever the QuorumStrategy says
          // in this impossible scenario is okay.

        case _ => fail // impossible

      }

      cluster1.assessAccepts(Seq(a98ack0, a99ack0)) shouldBe Option(QuorumAck)
      cluster1.assessAccepts(Seq(a98nack1, a99nack1)) shouldBe Option(QuorumNack)
    }
    def `should see a quorum of 2 in a cluster size of 3`: Unit = {
      val cluster1 = new DefaultQuorumStrategy(()=>3)
      cluster1.promiseQuorumSize shouldBe 2

      cluster1.assessAccepts(Seq(a98ack0, a98nack1)) shouldBe None
      cluster1.assessPromises(Seq(pAck, pNack)) shouldBe None

      cluster1.assessAccepts(Seq(a98ack0, a99ack0)) shouldBe Option(QuorumAck)
      cluster1.assessAccepts(Seq(a98nack1, a99nack1)) shouldBe Option(QuorumNack)
    }
    /**
      * 3 + 2 > 4 so proven to be safe in the FPaxos paper.
      */
    def `should perform the even nodes FPaxos optimistation in a cluster size of 4`: Unit = {
      val cluster1 = new DefaultQuorumStrategy(()=>4)
      cluster1.promiseQuorumSize shouldBe 3

      cluster1.assessAccepts(Seq(a98ack0, a99ack1)) shouldBe Option(QuorumAck)
      cluster1.assessAccepts(Seq(a98nack1, a99nack1)) shouldBe Option(QuorumNack)
    }
    /**
      * 4 + 3 > 6 so proven to be safe in the FPaxos paper.
      */
    def `should perform the even nodes FPaxos optimistation in a cluster size of 6`: Unit = {
      val cluster1 = new DefaultQuorumStrategy(()=>6)
      cluster1.promiseQuorumSize shouldBe 4

      cluster1.assessAccepts(Seq(a98ack0, a99ack1, a99ack2)) shouldBe Option(QuorumAck)
      cluster1.assessAccepts(Seq(a98nack0, a98nack1, a98nack2)) shouldBe Option(QuorumNack)
    }

  }

}
