package com.github.simbo1905.trexdemo

import akka.actor._
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import com.github.trex_paxos.demo.{ConsistentKVStore, MVStoreConsistentKVStore}
import com.github.trex_paxos.library.{Accept, Journal, JournalBounds, Progress}
import com.typesafe.config.ConfigFactory
import org.scalatest._
import matchers.should._
import org.h2.mvstore.MVStore
import org.scalatest.refspec.RefSpecLike

import scala.collection.immutable.{SortedMap, TreeMap}

object MapDBConsistentStoreTests {
  val config = ConfigFactory.parseString("trex.leader-timeout-min=50\ntrex.leader-timeout-max=300\nakka.loglevel = \"DEBUG\"\nakka.log-dead-letters-during-shutdown=false")
}

class InMemoryJournal extends Journal {
  var _progress = Journal.minBookwork.copy()
  var _map: SortedMap[Long, Accept] = TreeMap.empty

  def saveProgress(progress: Progress): Unit = _progress = progress

  def loadProgress(): Progress = _progress

  def accept(accepted: Accept*): Unit = accepted foreach { a =>
    _map = _map + (a.id.logIndex -> a)
  }

  def accepted(logIndex: Long): Option[Accept] = _map.get(logIndex)

  def bounds(): JournalBounds = {
    val keys = _map.keys
    if (keys.isEmpty) JournalBounds(0L, 0L) else JournalBounds(keys.head, keys.last)
  }
}

class MVStoreConsistentStoreTests extends TestKit(ActorSystem("LeaderSpec", MapDBConsistentStoreTests.config))
with DefaultTimeout with ImplicitSender with RefSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll with OptionValues {

  // an in memory store
  var mvstore: MVStore = new MVStore.Builder().open()

  before {
    mvstore = new MVStore.Builder().open()
  }

  after {
    mvstore = null
  }

  override def afterAll(): Unit = {
    shutdown()
  }

  object `Direct in-memory store` {

    object `can put, get and remove values` {
      val kvstore: ConsistentKVStore = new MVStoreConsistentKVStore(mvstore)
      kvstore.remove("hello") // noop
      kvstore.put("hello", "world")

      kvstore.get("hello") match {
        case Some((value, version)) =>
          value shouldBe("world")
          version shouldBe(1L)
        case x => fail(x.toString)
      }
      kvstore.remove("hello")
      kvstore.get("hello") shouldBe(None)
    }

    object `has oplock semantics` {
      val kvstore: ConsistentKVStore = new MVStoreConsistentKVStore(mvstore)
      kvstore.put("hello", "world", 10) shouldBe(false)
      kvstore.get("hello") shouldBe(None)
      kvstore.put("hello", "world", 0) shouldBe(true)
      kvstore.get("hello").value shouldBe(("world", 1))
      kvstore.put("hello", "world", 0) shouldBe(false)
      kvstore.put("hello", "world", 1) shouldBe(true)
      kvstore.get("hello").value shouldBe(("world", 2))
    }

  }

  object `Actor wrapped store` {

    object `can put, get and remove values` {

      val kvstore: ConsistentKVStore =
        TypedActor(system).typedActorOf(TypedProps(classOf[ConsistentKVStore],
          new MVStoreConsistentKVStore(mvstore)))

      kvstore.remove("hello") // noop
      kvstore.put("hello", "world")
      val (value, version) = kvstore.get("hello").getOrElse(fail())
      value shouldBe("world")
      version shouldBe(1L)
      kvstore.remove("hello")
      kvstore.get("hello") shouldBe(None)
    }

    object `has oplock semantics` {
      val kvstore: ConsistentKVStore =
        TypedActor(system).typedActorOf(TypedProps(classOf[ConsistentKVStore],
          new MVStoreConsistentKVStore(mvstore)))

      kvstore.put("hello", "world", 10) shouldBe(false)
      kvstore.get("hello") shouldBe(None)
      kvstore.put("hello", "world", 0) shouldBe(true)
      kvstore.get("hello").value shouldBe (("world", 1))
      kvstore.put("hello", "world", 0) shouldBe(false)
      kvstore.put("hello", "world", 1) shouldBe(true)
      kvstore.get("hello").value shouldBe (("world", 2))
    }

  }

}
