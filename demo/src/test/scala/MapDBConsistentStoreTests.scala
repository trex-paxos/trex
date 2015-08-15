package com.github.simbo1905.trexdemo

import akka.actor._
import akka.testkit.{TestFSMRef, ImplicitSender, DefaultTimeout, TestKit}
import com.github.simbo1905.trex._
import com.github.simbo1905.trex.internals.{Accept, ClusterMember, Progress, PaxosActor}
import com.typesafe.config.ConfigFactory
import org.scalatest._
import org.mapdb.{DB, DBMaker}

import scala.collection.immutable.{TreeMap, SortedMap}
import scala.collection.immutable.Seq

object MapDBConsistentStoreTests {
  val config = ConfigFactory.parseString("trex.leader-timeout-min=50\ntrex.leader-timeout-max=300\nakka.loglevel = \"DEBUG\"\nakka.log-dead-letters-during-shutdown=false")
}

class InMemoryJournal extends Journal {
  var _progress = Journal.minBookwork.copy()
  var _map: SortedMap[Long, Accept] = TreeMap.empty

  def save(progress: Progress): Unit = _progress = progress

  def load(): Progress = _progress

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
  with DefaultTimeout with ImplicitSender with SpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll {

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
      val (value, version) = store.get("hello").get
      value should be("world")
      version should be(1L)
      store.remove("hello")
      store.get("hello") match {
        case None => // success
        case x => fail(s"found $x when key should have been removed")
      }
    }

    object `has oplock semantics` {
      val store: ConsistentKVStore = new MapDBConsistentKVStore(db)
      store.put("hello", "world", 10) should be(false)
      store.get("hello") match {
        case None => // success
        case other => fail(s"$other")
      }
      store.put("hello", "world", 0) should be(true)
      store.get("hello") match {
        case None => fail
        case Some((data, version)) if version == 1 => // success
        case other => fail(s"$other")
      }
      store.put("hello", "world", 0) should be(false)
      store.put("hello", "world", 1) should be(true)
      store.get("hello") match {
        case None => fail
        case Some((data, version)) if version == 2 => // success
        case other => fail(s"$other")
      }
    }

  }

  object `Actor wrapped store` {

    object `can put, get and remove values` {

      val store: ConsistentKVStore =
        TypedActor(system).typedActorOf(TypedProps(classOf[ConsistentKVStore],
          new MapDBConsistentKVStore(db)))

      store.remove("hello") // noop
      store.put("hello", "world")
      val (value, version) = store.get("hello").get
      value should be("world")
      version should be(1L)
      store.remove("hello")
      store.get("hello") match {
        case None => // success
        case x => fail(s"found $x when key should have been removed")
      }
    }

    object `has oplock semantics` {
      val store: ConsistentKVStore =
        TypedActor(system).typedActorOf(TypedProps(classOf[ConsistentKVStore],
          new MapDBConsistentKVStore(db)))

      store.put("hello", "world", 10) should be(false)
      store.get("hello") match {
        case None => // success
        case other => fail(s"$other")
      }
      store.put("hello", "world", 0) should be(true)
      store.get("hello") match {
        case None => fail
        case Some((data, version)) if version == 1 => // success
        case other => fail(s"$other")
      }
      store.put("hello", "world", 0) should be(false)
      store.put("hello", "world", 1) should be(true)
      store.get("hello") match {
        case None => fail
        case Some((data, version)) if version == 2 => // success
        case other => fail(s"$other")
      }
    }

  }

  object `Driver wrapped store` {
    object `can put, get and remove values` {

      // the paxos actor nodes in our cluster
      var children = Map.empty[Int, ActorRef]

      var journals = Map.empty[Int,InMemoryJournal]

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