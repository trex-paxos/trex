package com.github.simbo1905.trex

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.github.simbo1905.trex.internals.{NoLongerLeaderException, ClientRequestCommandValue, CommandValue}
import org.scalatest._

import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

class LeaderStopsTests extends TestKit(ActorSystem("LeaderStops",
  NoFailureTests.spacedTimeoutConfig)) with SpecLike with ImplicitSender with BeforeAndAfterAll with BeforeAndAfter with Matchers {

  import scala.concurrent.duration._

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  var clusterHarness: TestActorRef[ClusterHarness] = null

  after {
    clusterHarness ! ClusterHarness.Halt
  }

  type Delivered = Seq[ArrayBuffer[CommandValue]]
  type Verifier = Delivered => Unit

  var data: Byte = 1

  def testLeaderDying(clusterSize: Int, verifier: Verifier): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    // given a test cluster harness sized to three nodes
    clusterHarness = TestActorRef(new ClusterHarness(clusterSize, NoFailureTests.spacedTimeoutConfig), "leaderCrash"+clusterSize)

    def send: Unit = {
      // when we sent it the application value of 1.toByte
      system.scheduler.scheduleOnce(400 millis, clusterHarness, ClientRequestCommandValue(0, Array[Byte](data)))

      // it commits and sends by the response of -1.toByte
      expectMsgPF(5 second) {
        case bytes: Array[Byte] if bytes(0) == -1 * data => // okay
        case nlle: NoLongerLeaderException => // okay
      }

      data = (data + 1).toByte
    }

    // reset the client data counter
    data = 1.toByte

    // we can commit a message
    send

    // then we kill a node
    clusterHarness ! "KillLeader"

    // we can still commit a message
    send

    // dig out the values which were committed
    val delivered: Seq[ArrayBuffer[CommandValue]] = clusterHarness.underlyingActor.delivered.values.toSeq

    // verify
    verifier(delivered)
  }

  object `A three node cluster` {
    val clusterSize = 3

    def `should survive the leader dying` {

      def verifier(delivered: Delivered): Unit = {
        // verify that two nodes committed the three nodes committed the second value
        val sizes: Seq[Int] = (delivered map {
          _.size
        }).sorted

        // first leader commits no_op then byte=1, second leader commits no_op then byte=2
        sizes should be (Seq(1,2,2))

        delivered foreach { d =>
          if( d.size == 1) {
            // ("killed leader delivered first byte")
            d.filter(_.isInstanceOf[ClientRequestCommandValue] ).head.asInstanceOf[ClientRequestCommandValue].bytes(0) == 1.toByte
          } else {
            // ("follower delivered both bytes")
            val delivered = d.filter(_.isInstanceOf[ClientRequestCommandValue] )
            delivered.map(_.asInstanceOf[ClientRequestCommandValue].bytes(0).toInt).sorted should be (Seq(1,2))
          }
        }

      }

      testLeaderDying(clusterSize, verifier)
    }
  }

  object `A five node cluster` {
    val clusterSize = 5

    def `should survive the leader dying` {

      def verifier(delivered: Delivered): Unit = {
        // verify that two nodes committed the three nodes committed the second value
        val sizes: Seq[Int] = (delivered map {
          _.size
        }).sorted

        // first leader commits noop then byte=1, second leader commits no_op then byte=2
        sizes should be (Seq(1,2,2,2,2))

        delivered foreach { d =>
          if( d.size == 1) {
            // ("killed leader delivered first byte")
            d.filter(_.isInstanceOf[ClientRequestCommandValue] ).headOption match {
              case Some(c: ClientRequestCommandValue) => c.bytes(0) == 1.toByte
              case x => fail(s"$x")
            }
          } else {
            // ("follower delivered both bytes")
            val delivered = d.filter(_.isInstanceOf[ClientRequestCommandValue] )
            delivered.map(_.asInstanceOf[ClientRequestCommandValue].bytes(0).toInt).sorted should be (Seq(1,2))
          }
        }

      }

      testLeaderDying(clusterSize, verifier)
    }
  }

  object `A seven node cluster` {
    val clusterSize = 7

    def `should survive the leader dying` {

      def verifier(delivered: Delivered): Unit = {
        // verify that two nodes committed the three nodes committed the second value
        val sizes: Seq[Int] = (delivered map {
          _.size
        }).sorted

        // first leader commits no_op then byte=1, second leader commits no_op then byte=2
        sizes should be (Seq(1,2,2,2,2,2,2))

        delivered foreach { d =>
          if( d.size == 1) {
            // ("killed leader delivered first byte")
            d.filter(_.isInstanceOf[ClientRequestCommandValue] ).headOption match {
              case Some(c: ClientRequestCommandValue) => c.bytes(0) == 1.toByte
              case x => fail(s"$x")
            }
          } else {
            // ("follower delivered both bytes")
            val delivered = d.filter(_.isInstanceOf[ClientRequestCommandValue] )
            delivered.map(_.asInstanceOf[ClientRequestCommandValue].bytes(0).toInt).sorted should be (Seq(1,2))
          }
        }

      }

      testLeaderDying(clusterSize, verifier)
    }
  }
}