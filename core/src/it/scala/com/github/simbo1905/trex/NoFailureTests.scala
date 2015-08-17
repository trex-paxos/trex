package com.github.simbo1905.trex

import akka.actor._
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.github.simbo1905.trex.internals.{ClientRequestCommandValue, CommandValue, NoLongerLeaderException}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, _}

import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

object NoFailureTests {
  val simultaneousTimeoutConfig = ConfigFactory.parseString("trex.leader-timeout-min=50\ntrex.leader-timeout-max=51\nakka.loglevel = \"DEBUG\"\nakka.log-dead-letters-during-shutdown=false")
  val spacedTimeoutConfig = ConfigFactory.parseString("trex.leader-timeout-min=50\ntrex.leader-timeout-max=300\nakka.loglevel = \"DEBUG\"\nakka.log-dead-letters-during-shutdown=false")
}

class NoFailureTests extends TestKit(ActorSystem("NoFailure",
  NoFailureTests.spacedTimeoutConfig)) with SpecLike with ImplicitSender with BeforeAndAfterAll with Matchers {

  import scala.concurrent.duration._

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  def checkNodes(delivered: Seq[ArrayBuffer[CommandValue]], clusterSize: Int): Unit = {
    0 until clusterSize foreach { i =>
      // NO_OPERATION are not delivered so should only have single value of 1
      delivered(i).size should be(1)
      delivered(i).head.asInstanceOf[ClientRequestCommandValue].bytes.length should be(1)
      delivered(i).head.asInstanceOf[ClientRequestCommandValue].bytes(0) should be(1.toByte)
    }
  }

  def runWithConfig(cfg: Config, name: String, clusterSize: Int): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    // given a test cluster harness sized to three nodes
    val ref = TestActorRef(new ClusterHarness(clusterSize, cfg),name)

    // when we sent it the application value of 1.toByte
    val expectedMsgId = 123456789L
    system.scheduler.scheduleOnce(400 millis, ref, ClientRequestCommandValue(expectedMsgId, Array[Byte](1)))

    // it commits and sends by the response of -1.toByte
    expectMsgPF(12 second) {
      case bytes: Array[Byte] if bytes(0) == -1 => // okay first leader committed
      case ex: NoLongerLeaderException if ex.msgId == expectedMsgId => // also okay first leader lost leadership
    }

    // dig out the values which were committed
    val delivered: Seq[ArrayBuffer[CommandValue]] = ref.underlyingActor.delivered.values.toSeq

    // kill off that cluster
    ref ! ClusterHarness.Halt

    checkNodes(delivered, clusterSize)
  }

  object `A three node cluster` {

    val clusterSize = 3

    def `should replicated values when timeouts are well spaced out` {
      runWithConfig(NoFailureTests.spacedTimeoutConfig, "goodTimeout"+clusterSize, clusterSize)
    }

    def `should replicated values when timeouts are not well spaced out` {
      runWithConfig(NoFailureTests.simultaneousTimeoutConfig, "poorTimeout"+clusterSize, clusterSize)
    }
  }

  object `A five node cluster` {
    val clusterSize = 5

    def `should replicated values when timeouts are well spaced out` {
      runWithConfig(NoFailureTests.spacedTimeoutConfig, "goodTimeout"+clusterSize, clusterSize)
    }

    def `should replicated values when timeouts are not well spaced out` {
      runWithConfig(NoFailureTests.simultaneousTimeoutConfig, "poorTimeout"+clusterSize, clusterSize)
    }
  }

  object `A seven node cluster` {
    val clusterSize = 7

    def `should replicated values when timeouts are well spaced out` {
      runWithConfig(NoFailureTests.spacedTimeoutConfig, "goodTimeout"+clusterSize, clusterSize)
    }

    def `should replicated values when timeouts are not well spaced out` {
      runWithConfig(NoFailureTests.simultaneousTimeoutConfig, "poorTimeout"+clusterSize, clusterSize)
    }
  }

}