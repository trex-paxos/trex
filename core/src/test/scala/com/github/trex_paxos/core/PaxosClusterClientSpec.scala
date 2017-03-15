package com.github.trex_paxos.core

import java.util.concurrent.TimeUnit

import com.github.trex_paxos.library._
import io.netty.buffer.ByteBuf
import org.scalatest.{AsyncWordSpec, Matchers, OptionValues}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.duration.{Duration, FiniteDuration}

class PaxosClusterClientSpec extends AsyncWordSpec with Matchers with OptionValues {

  val fiveMsDuration = FiniteDuration(5, TimeUnit.MILLISECONDS)

  abstract class TestPaxosClusterClient extends PaxosClusterClient {
    override def log: PaxosLogging = NoopLogging
  }

  "PaxosClusterClient" should {
    "should complete a request with a response" in {

      val outbound = ArrayBuffer[CommandValue]()

      val request = Array[Byte](98.toByte)
      val response = Array[Byte](99.toByte)

      val paxosClusterClient = new TestPaxosClusterClient {
        override def transmitToCluster(notLeaderCounter: Int, command: CommandValue): Unit = {
          outbound synchronized {
            outbound += command
            outbound.notifyAll()
          }
        }

        override def requestTimeout: Duration = fiveMsDuration

        override def maxAttempts: Int = Int.MaxValue
      }

      // send outbound
      val f: Future[ServerResponse] = paxosClusterClient.sendToCluster(request)

      // wait until it is actually sent
      outbound synchronized {
        if( outbound.size == 0 ) outbound.wait(250)
      }

      // outbound is sent to the server
      assert(outbound.headOption.value.bytes(0) == 98.toByte)

      // fake the response

      paxosClusterClient.receiveFromCluster(ServerResponse(0L, outbound.headOption.value.msgUuid, Some(response)))

      f map {
        _.response match {
          case Some(bytes) =>
            assert(bytes(0) == 99.toByte) // good
          case f => assert(false, f.toString)
        }
      }
    }
    "should complete the correct requests with multiple out of order responses" in {

      val outbound = ArrayBuffer[CommandValue]()

      val rq1 = Array[Byte](1.toByte)
      val rs1 = Array[Byte](11.toByte)
      val rq2 = Array[Byte](2.toByte)
      val rs2 = Array[Byte](22.toByte)
      val rq3 = Array[Byte](3.toByte)
      val rs3 = Array[Byte](33.toByte)

      val paxosClusterClient = new TestPaxosClusterClient {
        override def transmitToCluster(notLeaderCounter: Int, command: CommandValue): Unit = {
          outbound synchronized {
            outbound += command
            if( outbound.size >= 3) outbound.notifyAll()
          }
        }
        override def requestTimeout: Duration = fiveMsDuration

        override def maxAttempts: Int = Int.MaxValue
      }

      // send outbounds
      val f1 = paxosClusterClient.sendToCluster(rq1)
      val f2 = paxosClusterClient.sendToCluster(rq2)
      val f3 = paxosClusterClient.sendToCluster(rq3)

      // wait until they are all actually sent
      outbound synchronized {
        if( outbound.size < 3 ) outbound.wait(250)
      }

      // map of the bytes sent to the uuids of the commands
      val workToMsgUuid = (outbound map {
        case ClientCommandValue(msgUuid,bytes) =>
          bytes(0) -> msgUuid
        case f => fail(f.toString)
      }).toMap

      // fake the responses in reverse order
      paxosClusterClient.receiveFromCluster(ServerResponse(3L, workToMsgUuid(3.toByte), Some(rs3)))
      paxosClusterClient.receiveFromCluster(ServerResponse(2L, workToMsgUuid(2.toByte), Some(rs2)))
      paxosClusterClient.receiveFromCluster(ServerResponse(1L, workToMsgUuid(1.toByte), Some(rs1)))

      for {
        s3 <- f3
        s2 <- f2
        s1 <- f1
      } yield {
        assert {
          s1.response.headOption.value(0) == 11.toByte &&
          s2.response.headOption.value(0) == 22.toByte &&
          s3.response.headOption.value(0) == 33.toByte
        }
      }
    }
    "should roundtrip ClientCommandValue" in {
      Future{
        import PaxosClusterClient._
        val cmd = ClientCommandValue("_msguuid_", Array[Byte](99.toByte, 100.toByte))
        val byteBuf: ByteBuf = cmdToByteBuf(cmd)
        val cmd2 = byteBufToCmd(byteBuf)
        assert(cmd.msgUuid == cmd2.msgUuid && cmd2.bytes(0) == 99.toByte && cmd2.bytes(1) == 100.toByte)
      }
    }
  }
}
