package com.github.trex_paxos.util

import org.scalatest._
import matchers.should._

class PicklePositiveIntegersTests extends wordspec.AnyWordSpec with Matchers {

  import com.github.trex_paxos.util.PicklePositiveIntegers._

  "Pickling Bytes " should {
    "should have a good lookup table" in {

      constantsFor(0L) match {
        case `c0` => // good
        case f => fail(f.toString)
      }
      constantsFor(1L) match {
        case `c0` => // good
        case f => fail(f.toString)
      }
      constantsFor(50L) match {
        case `c0` => // good
        case f => fail(f.toString)
      }
      constantsFor(127L) match {
        case `c0` => // good
        case f => fail(f.toString)
      }

      constantsFor(128L) match {
        case `c1` => // good
        case f => fail(f.toString)
      }
      constantsFor(8192L) match {
        case `c1` => // good
        case f => fail(f.toString)
      }
      constantsFor(16383L) match {
        case `c1` => // good
        case f => fail(f.toString)
      }

      constantsFor(16384L) match {
        case `c2` => // good
        case f => fail(f.toString)
      }
      constantsFor(2*16384L+1) match {
        case `c2` => // good
        case f => fail(f.toString)
      }
      constantsFor(2097151L) match {
        case `c2` => // good
        case f => fail(f.toString)
      }

      constantsFor(2097152L) match {
        case `c3` => // good
        case f => fail(f.toString)
      }
      constantsFor(2*2097152L+1) match {
        case `c3` => // good
        case f => fail(f.toString)
      }
      constantsFor(134217728L - 1) match {
        case `c3` => // good
        case f => fail(f.toString)
      }

      constantsFor(268435456L) match {
        case `c4` => // good
        case f => fail(f.toString)
      }
      constantsFor(2*134217728L+1) match {
        case `c4` => // good
        case f => fail(f.toString)
      }
      constantsFor(34359738368L-1) match {
        case `c4` => // good
        case f => fail(f.toString)
      }


    }
    "should use compact integer binary form" in {
      // use 7 bits to encode smallest byte
      pickleInt(0).length should be (1)
      unpickleInt(pickleInt(0).iterator) should be(0)
      unpickleInt(pickleInt(1).iterator) should be(1)
      unpickleInt(pickleInt(127).iterator) should be(127)
      // use 14 bits to encode next smallest byte
      pickleInt(128).length should be (2)
      unpickleInt(pickleInt(128).iterator) should be(128)
      unpickleInt(pickleInt(8192).iterator) should be(8192)
      pickleInt(16384).length should be (3)
      unpickleInt(pickleInt(16384).iterator) should be(16384)
      unpickleInt(pickleInt(4*16384).iterator) should be(4*16384)

      pickleLong(72057594037927936L).length should be(9)
      unpickleLong(pickleLong(72057594037927936L).iterator) should be(72057594037927936L)

      unpickleLong(pickleLong(1483625023633L).iterator) should be(1483625023633L)

      pickleLong(Long.MaxValue).length should be(9)
      unpickleLong(pickleLong(Long.MaxValue).iterator) should be(Long.MaxValue)

      // numbers to test the byte boundaries
      val tests = (for {
        bits <- (1 until 63)
        large <- Seq(Math.pow(2, bits).toLong)
      } yield(Seq(large-1, large, large+1) )).flatten

      tests.foreach {
        l =>
          unpickleLong(pickleLong(l).iterator) should be(l)
      }

      tests.map(Long.MaxValue - _).foreach{
        l =>
          unpickleLong(pickleLong(l).iterator) should be(l)
      }

      // check lengths
      (1 until 63) foreach { bits =>
        val long = Math.pow(2, bits).toLong
        val length = 1 + bits / 7
        pickleLong(long).length should be(length)
      }
    }
  }
}
