package com.github.trex_paxos

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import com.github.trex_paxos.library._
import org.scalatest._

import scala.collection.immutable.Iterable
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.language.postfixOps
import scala.collection.mutable.Buffer

class LeaderStopsTests extends TestKit(ActorSystem("LeaderStops",
  NoFailureTests.spacedTimeoutConfig)) with SpecLike with ImplicitSender with BeforeAndAfterAll with BeforeAndAfter with Matchers {

  import scala.concurrent.duration._

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  val clusterHarness = new Box[TestActorRef[ClusterHarness]](None)

  after {
    clusterHarness() ! ClusterHarness.Halt
  }

  type Delivered = Map[Int, ArrayBuffer[Payload]]

  val data = new Box[Byte](Option(1.toByte))

  def testLeaderDying(clusterSize: Int): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    // given a test cluster harness sized to x nodes
    clusterHarness(TestActorRef(new ClusterHarness(clusterSize, NoFailureTests.spacedTimeoutConfig), "leaderCrash" + clusterSize))

    def send: Unit = {
      // when we sent it the application value of 1.toByte
      system.scheduler.scheduleOnce(200 millis, clusterHarness(), ClientRequestCommandValue(0, Array[Byte](data())))

      // it commits and sends by the response of -1.toByte
      expectMsgPF(2 second) {
        case bytes: Array[Byte] if bytes(0) == -1 * data() => // okay
        case nlle: NoLongerLeaderException => // okay
        case x => fail(x.toString)
      }

      data((1 + data()).toByte)
    }

    // reset the client data counter
    data(1.toByte)

    // we can commit a message
    send

    // then we kill a node
    clusterHarness() ! "KillLeader"

    // we can still commit a message
    send

    import akka.pattern.ask
    implicit val timeout = Timeout(2 seconds)

    awaitCond(check(clusterHarness().underlyingActor), 20 seconds, 200 millisecond)

    // shutdown the actor and have it tell us what was committed
    val future = clusterHarness().ask(ClusterHarness.Halt)

    val delivered = Await.result(future, 2 seconds).asInstanceOf[Map[Int, Buffer[Payload]]]

    consistentDeliveries(delivered)
  }

  // counts the number of delivered client bytes matches 2x cluster size - 1
  def check(cluster: ClusterHarness): Boolean = {

    val delivered: Seq[mutable.Buffer[Payload]] = cluster.delivered().values.toSeq

    val count = (0 until cluster.size).foldLeft(0){ (count, i) =>
      val found = delivered(i) map {
        case Payload(_, c: ClientRequestCommandValue) => 1
        case _ => 0
      }
      count + found.sum
    }
    count == 2 * cluster.size - 1
  }

  def consistentDeliveries(delivered: Map[Int, Buffer[Payload]]): Unit = {

    // slots are committed in ascending order with possible repeats and no gaps
    delivered.values foreach { values =>
      values.foldLeft(0L) {
        case (lastCommitted, Payload(nextCommitted, value)) =>
          lastCommitted should be <= nextCommitted
          if( nextCommitted > lastCommitted) {
            (nextCommitted - lastCommitted) shouldBe 1
          }
          nextCommitted
      }
    }

    case class LastValueAndCollected(last: Option[Payload], deduplicated: Seq[Payload])

    val noRepeats = delivered map {
      case (node, values) =>
        val deduplicated = values.foldLeft(LastValueAndCollected(None, Seq())) {
          case (LastValueAndCollected(last, deduplicated), next@Payload(nextIndex, value)) => last match {
            case Some(Payload(lastIndex, _)) if lastIndex == nextIndex =>
              LastValueAndCollected(Option(next), deduplicated)
            case _ =>
              LastValueAndCollected(Option(next), deduplicated :+ next)
          }
        }
        node -> deduplicated.deduplicated
    }

    // nodes must not see inconsistent commits but they may see less commits if they have been offline
    val nodes = noRepeats.keys
    (nodes.min until nodes.max) foreach { nodeId =>
      val previousNodeValues = noRepeats(nodeId)
      val nextNodeValues = noRepeats(nodeId + 1)
      val minSize = Seq(previousNodeValues.size, nextNodeValues.size).min
      previousNodeValues.take(minSize) shouldBe nextNodeValues.take(minSize)
    }

    val nonoops = delivered map {
      case (node, values) =>
        val nonoops = values.foldLeft(Seq[CommandValue]()) {
          case (seq, Payload(_, value)) => value match {
            case NoOperationCommandValue => seq
            case v => seq :+ v
          }
        }
        node -> nonoops
    }

    // notes either saw one byte committed else two bytes committed in that order
    (nodes.min until nodes.max) foreach { nodeId =>
      val values = nonoops(nodeId)
      values(0).bytes(0) shouldBe 1.toByte
      values.size match {
        case 1 => // okay
        case 2 => values(1).bytes(0) shouldBe 2.toByte
        case f => fail(values.toString())
      }
    }

    // some nodes saw both bytes committed
    val all = nonoops.flatMap {
      case (_, values) => values.map(_.bytes(0))
    }

    all should contain (1.toByte)
    all should contain (1.toByte)
  }

  object `A three node cluster` {
    val clusterSize = 3

    def `should survive the leader dying` {
      testLeaderDying(clusterSize)
    }
  }

  object `A four node cluster` {
    val clusterSize = 4

    def `should survive the leader dying` {
      testLeaderDying(clusterSize)
    }
  }

  object `A seven node cluster` {
    val clusterSize = 7

    def `should survive the leader dying` {
      testLeaderDying(clusterSize)
    }
  }

}