package com.github.trex_paxos.internals
import com.github.trex_paxos.akka.internals.{CommittedMembership, Member, MemberPickle, MemberStatus, Membership}
import org.scalatest._
import matchers.should._

class MemberPickleSpec extends wordspec.AnyWordSpec with Matchers {

  "Pickling simple objects " should {
    "roundtrip empty CommittedMembership" in {
      val cm = CommittedMembership(0L, Membership("", Seq()))
      val js = MemberPickle.toJson(cm)
      val cm2 = MemberPickle.fromJson(js)
      cm2 shouldBe Some(cm)
    }
    "roundtrip some CommittedMembership" in {
      val cm = CommittedMembership(99L, Membership("some", Seq(
        Member(1, "one", "two", MemberStatus.Learning),
        Member(2, "one2", "two2", MemberStatus.Accepting)
      )))
      val js = MemberPickle.toJson(cm)
      val cm2 = MemberPickle.fromJson(js)
      cm2 shouldBe Some(cm)
    }

  }
}
