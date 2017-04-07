package com.github.trex_paxos.core

import java.util.Arrays.{equals => bequals}

import com.github.trex_paxos.library._
import org.scalatest.{Matchers, WordSpecLike}

import scala.util.{Failure, Success, Try}

class DrivePickleTests extends WordSpecLike with Matchers {

  "Driver pickling rich objects" should {
    "roundtrip None ServerResponse" in {
      val s1 = ServerResponse(0, "one", None)
      DriverPickle.unpack(DriverPickle.pack(s1).iterator) match {
        case `s1` => // good
        case f => fail(f.toString)
      }
    }
    "roundtrip empty ServerResponse" in {
      val s1 = ServerResponse(0, "one", Some(Array[Byte]()))
      DriverPickle.unpack(DriverPickle.pack(s1).iterator) match {
        case ServerResponse(n, m, Some(a)) if n == 0L && m == "one" && a.length == 0 => // good
        case f => fail(f.toString)
      }
    }
    val bytes1 = Array[Byte](5, 6)
    "roundtrip some ServerResponse" in {
      val s1 = ServerResponse(0, "one", Some(bytes1))
      DriverPickle.unpack(DriverPickle.pack(s1).iterator) match {
        case ServerResponse(n, m, Some(bout)) if n == 0L && m == "one" =>
          assert(bequals(Array[Byte](5, 6), bout))
        case f => fail(f.toString)
      }
    }

  }
}