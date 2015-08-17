package com.github.simbo1905.trex.internals

import java.io.File

import com.github.simbo1905.trex.JournalBounds
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfter, Matchers, WordSpecLike}

class FileJournalSpec extends WordSpecLike with Matchers with BeforeAndAfter with MockFactory {

  // logical store file creates two other files .p and .t which we delete
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
        val accept = Accept(Identifier(1, minValue, 0), MembershipCommandValue(99L, Seq(ClusterMember(0, "zero", true), ClusterMember(1, "one", false))))
        val bytes = Pickle.pack(accept)
        val parsed = Pickle.unpack(bytes).asInstanceOf[Accept]
        assert(parsed.id == accept.id)
        val membership: MembershipCommandValue = parsed.value.asInstanceOf[MembershipCommandValue]
        assert(membership.msgId == 99L)
        assert(membership.members == Seq(ClusterMember(0, "zero", true), ClusterMember(1, "one", false)))
      }
    }
  }

  val expectedString = "Knossos"
  val expectedBytes = expectedString.getBytes

  def actualString(bytes: Array[Byte]) = new String(bytes)

  "FileJournal" should {
    "make bookwork durable" in {
      val number = BallotNumber(10, 2)
      val bookwork = Progress(number.copy(counter = number.counter), Identifier(1, number, 88L))

      {
        val j = new FileJournal(storeFile, 10)
        j.save(bookwork)
        j.close()
      }

      val j = new FileJournal(storeFile, 10)
      val readBackData = j.load()
      j.close()
      assert(bookwork == readBackData)
    }
    "make accept durable" in {
      val high = BallotNumber(10, 2)
      val logIndex = 0L
      val identifier = Identifier(1, high, logIndex)
      val accept = Accept(identifier, ClientRequestCommandValue(0, expectedBytes))

      {
        val j = new FileJournal(storeFile, 10)
        j.accept(accept)
        j.close()
      }

      val j = new FileJournal(storeFile, 10)
      val readBackAccept = j.accepted(logIndex).get
      j.close()
      assert(java.util.Arrays.equals(Pickle.pickle(accept).toArray, Pickle.pickle(readBackAccept).toArray))
    }
    "overwrite old values" in {
      var n = 0
      def next = {
        val high = BallotNumber(n, n)
        val logIndex = n.toLong
        val identifier = Identifier(1, high, logIndex)
        n = n + 1
        Accept(identifier, ClientRequestCommandValue(0, expectedBytes))
      }

      val j = new FileJournal(storeFile, 2)

      for (a <- 0 to 9) j.accept(next)

      j.save(Progress(BallotNumber(n, n), Identifier(1, BallotNumber(n, n), 5)))

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
      }).toSet

      3 to 9 foreach { index =>
        assert( indexes.contains(index))
      }
    }
    "return the bounds of the keys" in {
      var n = 100
      def next = {
        val high = BallotNumber(n, n)
        val logIndex = n.toLong
        val identifier = Identifier(1, high, logIndex)
        n = n + 1
        Accept(identifier, ClientRequestCommandValue(0, expectedBytes))
      }

      val j = new FileJournal(storeFile, 2)

      for (a <- 0 to 9) j.accept(next)

      assert(j.bounds == JournalBounds(100, 109))

      j.close()
    }
  }
}