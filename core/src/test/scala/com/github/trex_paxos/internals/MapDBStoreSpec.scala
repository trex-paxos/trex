package com.github.trex_paxos.internals

import java.io.File
import com.github.trex_paxos.library._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfter, Matchers, WordSpecLike}

class MapDBStoreSpec extends WordSpecLike with Matchers with BeforeAndAfter with MockFactory {

  // MapDB 1.0.x logical store file creates two other files .p and .t which we delete
  val storeFile = new File("store.trex")
  val storeFileP = new File("store.trex.p")
  val storeFileT = new File("store.trex.t")

  def deleteMapDbFiles: Unit = {
    if (storeFile.exists()) storeFile.delete()
    if (storeFileP.exists()) storeFileP.delete()
    if (storeFileT.exists()) storeFileT.delete()
  }

  before {
    deleteMapDbFiles
  }

  after {
    deleteMapDbFiles
  }

  val minValue = BallotNumber(Int.MinValue, Int.MinValue)

  "Pickling" should {
    "round trip a bookwork literal" in {
      val minBookwork = Progress(minValue, Identifier(1, minValue, 0))
      val bytes = Pickle.pack(minBookwork)
      val parsed = Pickle.unpack(bytes)
      assert(parsed == minBookwork)
    }
    "Roundtrip different Value types" in {
      {
        val noop = Accept(Identifier(1, minValue, 0), NoOperationCommandValue)
        val bytes = Pickle.pack(noop)
        val parsed = Pickle.unpack(bytes)
        assert(parsed == noop)
      }
      {
        val client = Accept(Identifier(1, minValue, 0), ClientRequestCommandValue(0, "hello".getBytes("UTF8")))
        val bytes = Pickle.pack(client)
        val parsed = Pickle.unpack(bytes).asInstanceOf[Accept]
        assert(parsed.id == client.id)
        assert(parsed.value.isInstanceOf[ClientRequestCommandValue])
        assert(new String(parsed.value.asInstanceOf[ClientRequestCommandValue].bytes, "UTF8") == "hello")
      }
      {
        val accept = Accept(Identifier(1, minValue, 0), MembershipCommandValue(99L, Seq(Member(0, "zero", Accepting), Member(1, "one", Departed))))
        val bytes = Pickle.pack(accept)
        val parsed = Pickle.unpack(bytes).asInstanceOf[Accept]
        assert(parsed.id == accept.id)
        val membership: MembershipCommandValue = parsed.value.asInstanceOf[MembershipCommandValue]
        assert(membership.msgId == 99L)
        assert(membership.members == Seq(Member(0, "zero", Accepting), Member(1, "one", Departed)))
      }
    }
  }

  val expectedString = "Knossos"
  val expectedBytes = expectedString.getBytes

  def actualString(bytes: Array[Byte]) = new String(bytes)

  "MapDBStore" should {
    "make bookwork durable" in {
      val number = BallotNumber(10, 2)
      val bookwork = Progress(number.copy(counter = number.counter), Identifier(1, number, 88L))

      {
        val j = new MapDBStore(storeFile, 10)
        j.saveProgress(bookwork)
        j.close()
      }

      val j = new MapDBStore(storeFile, 10)
      val readBackData = j.loadProgress()
      j.close()
      assert(bookwork == readBackData)
    }
    "make accept durable" in {
      val high = BallotNumber(10, 2)
      val logIndex = 0L
      val identifier = Identifier(1, high, logIndex)
      val accept = Accept(identifier, ClientRequestCommandValue(0, expectedBytes))

      {
        val j = new MapDBStore(storeFile, 10)
        j.accept(accept)
        j.close()
      }

      val j = new MapDBStore(storeFile, 10)
      val readBackAccept = j.accepted(logIndex).getOrElse(fail("should be defined"))
      j.close()
      assert(java.util.Arrays.equals(Pickle.pickle(accept).toArray, Pickle.pickle(readBackAccept).toArray))
    }
    "overwrite old values" in {
      val n = Box(0)
      def next = {
        val high = BallotNumber(n(), n())
        val logIndex = n().toLong
        val identifier = Identifier(1, high, logIndex)
        n(n() + 1)
        Accept(identifier, ClientRequestCommandValue(0, expectedBytes))
      }

      val j = new MapDBStore(storeFile, 2)

      for (a <- 0 to 9) j.accept(next)

      j.saveProgress(Progress(BallotNumber(n(), n()), Identifier(1, BallotNumber(n(), n()), 5)))

      val found = 1 to 10 flatMap {
        j.accepted(_)
      }

      j.close()

      assert(7 == found.length)

      val indexes = (found map {
        case Accept(Identifier(1, BallotNumber(a, b), index), ClientRequestCommandValue(0, bytes)) =>
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
        Accept(identifier, ClientRequestCommandValue(0, expectedBytes))
      }

      val j = new MapDBStore(storeFile, 2)

      for (a <- 0 to 9) j.accept(next)

      assert(j.bounds == JournalBounds(100, 109))

      j.close()
    }
    "load nothing when empty" in {
      val store = new MapDBStore(storeFile, 2)
      store.loadMembership() shouldBe None
    }
    "make a membership durable" in {
      val m = Membership(Seq(Member(1, "one", Learning), Member(2, "two", Accepting)))

      {
        val store = new MapDBStore(storeFile, 2)
        store.saveMembership(0L, m)
      }

      {
        val store = new MapDBStore(storeFile, 2)
        store.loadMembership() shouldBe Some(m)
      }
    }
    "should throw an exception for an overwrite" in {
      val m = Membership(Seq(Member(1, "one", Learning), Member(2, "two", Accepting)))
      val store = new MapDBStore(storeFile, 2)
      store.saveMembership(0L, m)
      try {
        store.saveMembership(0L, m)
        fail
      } catch {
        case _ :Exception => // good
      }
    }
    "should return the highest value saved" in {
      val m1 = Membership(Seq(Member(1, "one", Learning), Member(2, "two", Accepting)))
      val m2 = Membership(Seq(Member(2, "two", Departed), Member(3, "three", Accepting)))
      val store = new MapDBStore(storeFile, 2)
      store.saveMembership(99L, m1)
      store.saveMembership(999L, m2)
      store.loadMembership() shouldBe Some(m2)
    }
  }
}