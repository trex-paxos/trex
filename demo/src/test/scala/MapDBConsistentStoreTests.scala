package com.github.simbo1905.trexdemo

import akka.actor._
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import com.github.trex_paxos.demo.{ConsistentKVStore, MapDBConsistentKVStore}
import com.github.trex_paxos.library.{Accept, Journal, JournalBounds, Progress}
import com.typesafe.config.ConfigFactory
import org.mapdb.{DB, DBMaker}
import org.scalatest._
import matchers.should._

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

  def bounds: JournalBounds = {
    val keys = _map.keys
    if (keys.isEmpty) JournalBounds(0L, 0L) else JournalBounds(keys.head, keys.last)
  }
}

class MapDBConsistentStoreTests extends TestKit(ActorSystem("LeaderSpec", MapDBConsistentStoreTests.config))
with DefaultTimeout with ImplicitSender with RefSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll with OptionValues {

  var db: DB = DBMaker.newMemoryDB().make()

  before {
    db = DBMaker.newMemoryDB().make()
  }

  after {
    db = null
  }

  override def afterAll() {
    shutdown()
  }

  object `Direct in-memory store` {

    object `can put, get and remove values` {
      val store: ConsistentKVStore = new MapDBConsistentKVStore(db)
      store.remove("hello") // noop
      store.put("hello", "world")

      store.get("hello") match {
        case Some((value, version)) =>
          value shouldBe("world")
          version shouldBe(1L)
        case x => fail(x.toString)
      }
      store.remove("hello")
      store.get("hello") shouldBe(None)
    }

    object `has oplock semantics` {
      val store: ConsistentKVStore = new MapDBConsistentKVStore(db)
      store.put("hello", "world", 10) shouldBe(false)
      store.get("hello") shouldBe(None)
      store.put("hello", "world", 0) shouldBe(true)
      store.get("hello").value shouldBe(("world", 1))
      store.put("hello", "world", 0) shouldBe(false)
      store.put("hello", "world", 1) shouldBe(true)
      store.get("hello").value shouldBe(("world", 2))
    }

  }

  object `Actor wrapped store` {

    object `can put, get and remove values` {

      val store: ConsistentKVStore =
        TypedActor(system).typedActorOf(TypedProps(classOf[ConsistentKVStore],
          new MapDBConsistentKVStore(db)))

      store.remove("hello") // noop
      store.put("hello", "world")
      val (value, version) = store.get("hello").getOrElse(fail())
      value shouldBe("world")
      version shouldBe(1L)
      store.remove("hello")
      store.get("hello") shouldBe(None)
    }

    object `has oplock semantics` {
      val store: ConsistentKVStore =
        TypedActor(system).typedActorOf(TypedProps(classOf[ConsistentKVStore],
          new MapDBConsistentKVStore(db)))

      store.put("hello", "world", 10) shouldBe(false)
      store.get("hello") shouldBe(None)
      store.put("hello", "world", 0) shouldBe(true)
      store.get("hello").value shouldBe (("world", 1))
      store.put("hello", "world", 0) shouldBe(false)
      store.put("hello", "world", 1) shouldBe(true)
      store.get("hello").value shouldBe (("world", 2))
    }

  }

  object `Driver wrapped store` {

    object `can put, get and remove values` {

      // the paxos actor nodes in our cluster
      var children = Map.empty[Int, ActorRef]

      var journals = Map.empty[Int, InMemoryJournal]

      val size = 3

      //      (0 until size) foreach { i =>
      //        val node = new InMemoryJournal
      //        journals = journals + (i -> node)
      //        val actor: ActorRef = system.actorOf(Props(classOf[TestPaxosActorWithTimeout],
      //          PaxosActor.Configuration(MapDBConsistentStoreTests.config, size), i, self, node, node.deliver, recordTraceData _))
      //        children = children + (i -> actor)
      //        log.info(s"$i -> $actor")
      //        lastLeader = actor
      //        tracedData = tracedData + (i -> Seq.empty)
      //      }

    }

  }

}
