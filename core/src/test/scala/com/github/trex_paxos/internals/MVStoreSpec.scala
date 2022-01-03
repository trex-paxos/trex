package com.github.trex_paxos.internals

import java.io.File

import _root_.com.github.trex_paxos.library._
import org.scalatest._
import matchers.should._

class MVStoreSpec extends wordspec.AnyWordSpec with Matchers with BeforeAndAfter  {

  val storeFile = new File("store.trex")

  def deleteMapDbFiles: Unit = {
    if (storeFile.exists()) storeFile.delete()
  }

  before {
    deleteMapDbFiles
  }

  after {
    deleteMapDbFiles
  }

  val minValue = BallotNumber(0, 0)

  val expectedString = "Knossos"
  val expectedBytes = expectedString.getBytes

  def actualString(bytes: Array[Byte]) = new String(bytes)

  "MVStoreJournal" should {
    "make bookwork durable" in {
      val number = BallotNumber(10, 2)
      val bookwork = Progress(number.copy(counter = number.counter), Identifier(1, number, 88L))

      {
        val j = new MVStoreJournal(storeFile, 10)
        j.saveProgress(bookwork)
        j.close()
      }

      val j = new MVStoreJournal(storeFile, 10)
      val readBackData = j.loadProgress()
      j.close()
      assert(bookwork == readBackData)
    }
    "make accept durable" in {
      val high = BallotNumber(10, 2)
      val logIndex = 0L
      val identifier = Identifier(1, high, logIndex)
      val accept = Accept(identifier, ClientCommandValue("0", expectedBytes))

      {
        val j = new MVStoreJournal(storeFile, 10)
        j.accept(accept)
        j.close()
      }

      val j = new MVStoreJournal(storeFile, 10)
      val readBackAccept = j.accepted(logIndex).getOrElse(fail("should be defined"))
      j.close()
    }
    "overwrite old values" in {
      val n = Box(0)
      def next = {
        val high = BallotNumber(n(), n())
        val logIndex = n().toLong
        val identifier = Identifier(1, high, logIndex)
        n(n() + 1)
        Accept(identifier, ClientCommandValue("0", expectedBytes))
      }

      val j = new MVStoreJournal(storeFile, 2)

      for (a <- 0 to 9) j.accept(next)

      j.saveProgress(Progress(BallotNumber(n(), n()), Identifier(1, BallotNumber(n(), n()), 5)))

      val found = 1 to 10 flatMap {
        j.accepted(_)
      }

      j.close()

      assert(7 == found.length)

      val indexes = (found map {
        case Accept(Identifier(1, BallotNumber(a, b), index), ClientCommandValue("0", bytes)) =>
          assert(java.util.Arrays.equals(bytes, expectedBytes))
          assert(a == b)
          assert(index == a)
          index
        case f => fail(f.toString)
      }).toSet

      3 to 9 foreach { index =>
        assert( indexes.contains(index))
      }
    }
    "return the bounds of the keys" in {
      val n = Box(100)
      def next = {
        val high = BallotNumber(n(), n())
        val logIndex = n().toLong
        val identifier = Identifier(1, high, logIndex)
        n(n() + 1)
        Accept(identifier, ClientCommandValue("0", expectedBytes))
      }

      val j = new MVStoreJournal(storeFile, 2)

      for (a <- 0 to 9) j.accept(next)

      assert(j.bounds() == JournalBounds(100, 109))

      j.close()
    }
    "load nothing when empty" in {
      val store = new MVStoreJournal(storeFile, 2)
      store.loadMembership() shouldBe None
    }
    "should throw an exception for an overwrite" in {
      val m = Membership("default", Seq(Member(1, "one", "xxx", MemberStatus.Learning), Member(2, "two", "yyy", MemberStatus.Accepting)))
      val store = new MVStoreJournal(storeFile, 2)
      store.saveMembership(CommittedMembership(0L, m))
      try {
        store.saveMembership(CommittedMembership(0L, m))
        fail()
      } catch {
        case _ :Exception => // good
      }
    }
  }
}
