package com.github.trex_paxos.internals

import akka.actor.{ActorContext, ActorSystem, Props, TypedActor, TypedProps}
import akka.util.Timeout
import com.github.trex_paxos.library.{Accept, Journal, JournalBounds, Progress}
import com.github.trex_paxos._
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.{BeforeAndAfterAll, Matchers, SpecLike}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.collection.mutable

object EndToEndSpec {

  val cstring =
    """trex.leader-timeout-min=1
      |trex.leader-timeout-max=10
      |akka.loglevel = "DEBUG"
      |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      |akka.remote {
      |    netty.tcp {
      |      hostname = "127.0.0.1"
      |    }
      | }
      | actor {
      |    debug {
      |      lifecycle = on
      |      unhandled = on
      |    }
      |  }
    """.stripMargin

  val config = ConfigFactory.parseString(cstring)
}

class TestStore extends Journal with TrexMembership {

  val pStore = new mutable.HashSet[Progress]()

  override def saveProgress(progress: Progress): Unit = pStore.add(progress)

  override def loadProgress(): Progress = pStore.headOption.getOrElse(Journal.minBookwork)

  val aStore = new mutable.HashMap[Long, Accept]()

  override def accept(as: Accept*): Unit = as foreach {
    case a => aStore.put(a.id.logIndex, a)
  }

  override def accepted(logIndex: Long): Option[Accept] = aStore.get(logIndex)

  override def bounds: JournalBounds = if (aStore.isEmpty) {
    JournalBounds(0L, 0L)
  } else {
    JournalBounds(aStore.keySet.min, aStore.keySet.max)
  }

  val mStore = new mutable.HashMap[Long, Membership]()

  override def saveMembership(slot: Long, membership: Membership): Unit = mStore.put(slot, membership)

  override def loadMembership(): Option[Membership] = mStore.keys match {
    case ks if ks.isEmpty => None
    case ks => mStore.get(ks.max)
  }
}

trait Register {
  def in(v: String): Unit

  def out(): Option[String]
}

class TestRegister extends Register {
  val r = collection.mutable.HashSet[String]()

  override def in(v: String): Unit = {
    r.clear()
    r.add(v)
  }

  override def out(): Option[String] = r.headOption
}

class EndToEndSpec extends SpecLike with BeforeAndAfterAll with Matchers {

  val system1 =
    ActorSystem("EndToEndSpec", ConfigFactory.load(EndToEndSpec.config).withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(11111)))
  val system2 =
    ActorSystem("EndToEndSpec", ConfigFactory.load(EndToEndSpec.config).withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(33333)))
  val system3 = ActorSystem("EndToEndSpec", EndToEndSpec.config)

  override def afterAll(): Unit = {
    system1.shutdown()
    system2.shutdown()
    system3.shutdown()
  }

  object `a cluster` {

    def `will replicate a value`: Unit = {

      val membership = Membership(Seq(Member(1, "127.0.0.1:22222", Accepting), Member(2, "127.0.0.1:44444", Accepting)))

      val node1 = Node(1, "127.0.0.1", 11111, 22222)
      val node2 = Node(2, "127.0.0.1", 33333, 44444)

      val conf = PaxosProperties()
      val store1 = new TestStore
      val target1 = new TestRegister
      store1.saveMembership(1L, membership)

      system1.actorOf(TrexServer(conf, classOf[TypedActorPaxosEndpoint2], node1, store1, store1, target1), "PaxosActor")

      val store2 = new TestStore
      val target2 = new TestRegister
      store2.saveMembership(1L, membership)

      system2.actorOf(TrexServer(conf, classOf[TypedActorPaxosEndpoint2], node2, store2, store2, target2), "PaxosActor")

      val timeout = Timeout(100 millisecond)

      val cluster = Cluster("EndToEndSpec", "/tmp", 1000, Seq(node1, node2))

      val driver = system3.actorOf(Props(classOf[StaticClusterDriver], timeout, cluster, 20, BaseDriver.defaultSelectionUrlFactory _), "TrexDriver")

      val typedActor: Register =
        TypedActor(system3).
          typedActorOf(
            TypedProps[Register],
            driver)

      typedActor.in("hello")
      typedActor.out() shouldBe Some("hello")

      target1.out() shouldBe Some("hello")
      target2.out() shouldBe Some("hello")

    }
  }

}