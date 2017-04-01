package com.github.trex_paxos

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import com.github.trex_paxos.library._
import org.scalatest._
import org.scalatest.refspec.RefSpecLike

import scala.collection.mutable.{ArrayBuffer, Buffer}
import scala.concurrent.Await
import scala.language.postfixOps
import akka.event.{LogSource, Logging}
import com.typesafe.config.ConfigFactory

object LeaderStopsTests {
  val simultaneousTimeoutConfig = ConfigFactory.parseString("trex.leader-timeout-min=50\ntrex.leader-timeout-max=51\nakka.loglevel = \"DEBUG\"\nakka.log-dead-letters-during-shutdown=false")
  val spacedTimeoutConfig = ConfigFactory.parseString("trex.leader-timeout-min=50\ntrex.leader-timeout-max=300\nakka.loglevel = \"DEBUG\"\nakka.log-dead-letters-during-shutdown=false")
}

class LeaderStopsTests extends TestKit(ActorSystem("LeaderStops",
  LeaderStopsTests.spacedTimeoutConfig)) with RefSpecLike with ImplicitSender with BeforeAndAfterAll with BeforeAndAfter with Matchers {

  implicit val myLogSourceType: LogSource[LeaderStopsTests] = new LogSource[LeaderStopsTests] {
    def genString(a: LeaderStopsTests) = "LeaderStopsTests"
  }

  import scala.concurrent.duration._

  val logger = Logging(system, this)

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  val clusterHarness = new Box[TestActorRef[ClusterHarness]](None)

  after {
    clusterHarness() ! ClusterHarness.Halt
  }

  type Delivered = Map[Int, ArrayBuffer[Payload]]

  val responses = scala.collection.mutable.HashMap[Byte, Any]()

  def testLeaderDying(clusterSize: Int): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    logger.info(s"starting testLeaderDying for clusterSize ${clusterSize}")

    // given a test cluster harness sized to x nodes
    clusterHarness(TestActorRef(new ClusterHarness(clusterSize, LeaderStopsTests.spacedTimeoutConfig), "leaderCrash" + clusterSize))

    def send(b: Byte): Unit = {
      val c = ClientCommandValue(b.toString, Array[Byte](b))

      logger.info(s"injecting $c")

      // when we sent it the application value of 1.toByte
      system.scheduler.scheduleOnce(100 millis, clusterHarness(), c)

      // it commits and sends by the response of -1.toByte
      expectMsgPF(2 second) {
        case bytes: Array[Byte] if bytes(0) == -1 * b =>
          logger.info("got back successful echo response")
          responses.put(b, bytes(0))
        case lle: LostLeadershipException =>
          logger.info("got back LostLeadershipException")
          responses.put(b, lle)
        case x =>
          logger.error(s"got back unexpected $x")
          fail(x.toString)
      }
    }

    // reset seen responses
    responses.clear()

    logger.info(s"sending first client message")

    // get a response from a leader
    send(1.toByte)

    awaitCond(responses.size > 0, 20 seconds, 200 millisecond)

    logger.info(s"responses: ${responses}")

    logger.info(s"killing the leader")

    // kill the leader
    clusterHarness() ! "KillLeader"

    logger.info(s"sending second client message")

    // we can still get a response from a new leader
    send(2.toByte)

    awaitCond(responses.size > 1, 20 seconds, 200 millisecond)

    logger.info(s"responses: ${responses}")

    import akka.pattern.ask
    implicit val timeout = Timeout(2 seconds)

    // shutdown the actor and have it tell us what was committed
    val future = clusterHarness().ask(ClusterHarness.Halt)

    val delivered = Await.result(future, 2 seconds).asInstanceOf[Map[Int, Buffer[Payload]]]

    logger.info(s"checking the deliveries: ${delivered}")

    consistentDeliveries(delivered)
    logger.info(s"all done")
  }

  def consistentDeliveries(delivered: Map[Int, Buffer[Payload]]): Unit = {

    // slots are committed in ascending order.
    // note that we will see gaps due to chosen Noops during leader takeovers which are not delivered.
    delivered.values foreach { values =>
      values.foldLeft(0L) {
        case (lastCommitted, Payload(Identifier(_, _, nextCommitted), value)) =>
          lastCommitted should be <= nextCommitted
          nextCommitted
      }
    }

    case class LastValueAndCollected(last: Option[Payload], deduplicated: Seq[Payload])

    // not sure that this deduplication is necessary with current tests.
    // in the wild repeats are posible if the application callbacks and the journal are not managed by an external transation.
    // in practice with these in-memory tests we are not going to see crash scenario which lead to repeats
    val noRepeats: Map[Int, Seq[Payload]] = delivered map {
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