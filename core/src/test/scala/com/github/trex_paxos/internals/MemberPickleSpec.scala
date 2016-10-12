package com.github.trex_paxos.internals


package com.github.trex_paxos.internals

import org.scalatest.{Matchers, WordSpecLike}

import java.util.Arrays.{equals => bequals}

class MemberPickleSpec extends WordSpecLike with Matchers {

  "Pickling rich objects" should {

//    "roundtrip member" in {
//      val m = Member(111, "one", "two", Learning)
//      val in = MemberPickle.pickle(m)
//      MemberPickle.unpickleMember(in) match {
//        case (`m`, _) =>
//        case f =>
//          fail(f.toString)
//      }
//    }
//    "roundtrip membership" in {
//      val m = Membership("mycluster", Seq(Member(111, "one", "two", Learning), Member(222, "three", "four", Departed)))
//      MemberPickle.unpack(MemberPickle.pack(m)) match {
//        case `m` =>
//        case f => fail(f.toString)
//      }
//    }
//    "roundtrip membership query" in {
//      val q = MembershipQuery(8888L)
//      MemberPickle.unpack(MemberPickle.pack(q)) match {
//        case `q` =>
//        case f => fail(f.toString)
//      }
//    }

  }
}