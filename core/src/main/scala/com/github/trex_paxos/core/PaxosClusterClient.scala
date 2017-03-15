package com.github.trex_paxos.core

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListMap}

import com.github.trex_paxos.library.{ClientCommandValue, CommandValue, PaxosLogging, ServerResponse}
import com.github.trex_paxos.util.Pickle
import io.netty.buffer.ByteBuf

import scala.collection._
import scala.collection.concurrent.TrieMap
import scala.collection.convert.decorateAsScala._
import scala.compat.Platform
import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Promise}
import scala.util.Success

/**
  * Bookwork to hold a request.
  *
  * @param command          The command in the request so that we can resend. This assumes that you have idempotency if not you should set the maxAttempts to zero.
  * @param promise          The callback.
  * @param timeoutTime      The minimum timeout point in ms.
  * @param attempt          If we allow multiple attempts this is the retry countdown.
  * @param notLeaderCounter The counter value when the request was sent.
  */
case class Request(command: CommandValue, promise: Promise[ServerResponse], timeoutTime: Long, attempt: Int, notLeaderCounter: Int)

/**
  * A meeting point for messages exchanged with the paxos cluster with timeout and retry logic.
  */
trait PaxosClusterClient {

  def log: PaxosLogging

  /**
    * @return The timeout at which point our future returns a timeout exception. This means that we dont know
    *                       whether the command was actually run. During a leader failover the value may be selected but
    *                       as our connection to the dead leader is gone we won't get notified that the command was run.
    *                       Instead the application has to requery.
    */
  def requestTimeout: Duration

  /**
    * @return The number of retry attempts. Should be set to zero if the messages are not idempotent
    */
  def maxAttempts: Int

  val timeoutMillis = requestTimeout.toMillis

  /**
    * lookup a request by its identifier
    */
  private[this] val requestById: concurrent.Map[String, Request] = new ConcurrentHashMap[String, Request]().asScala

  /**
    * look up requests by the timeout then by identifier
    * due to clock resolution we can have multiple requests timeout in the same millisecond
    */
  private[this] val requestByTimeoutById: java.util.SortedMap[Long, concurrent.Map[String, Request]] = new ConcurrentSkipListMap[Long, concurrent.Map[String, Request]]

  /**
    * The message ID is used to correlate responses back from the paxos cluster with the request sent out.
    *
    * @return
    */
  def nextMessageUuid() = java.util.UUID.randomUUID.toString

  /**
    * Paxos is optimal with a stable leader. The leader may change arbitrarily. This means that we may get back a
    * NotLeader for a given message. When we see that we increment the following counter. That means that the
    * transmission strategy can pick another node to try. A naive strategy would be to pick
    * a node using "notLeaderCounter % clusterSize" but a more sophisticated strategies might be possible.
    */
  @volatile protected var notLeaderCounter: Int = 0

  def hold(request: Request): Unit = {
    // this should overwrite an old request if we timed out and are retrying with a new timeout
    requestById.put(request.command.msgUuid, request)

    requestByTimeoutById.synchronized {
      Option(requestByTimeoutById.get(request.timeoutTime)) match {
        case Some(requestsAtTimeout) =>
          // this may overwrite an old request if we timed out and are retrying
          requestsAtTimeout.put(request.command.msgUuid, request)
        case None =>
          val requestsAtTimeout = TrieMap[String, Request]()
          requestsAtTimeout.put(request.command.msgUuid, request)
          requestByTimeoutById.put(request.timeoutTime, requestsAtTimeout)
      }
    }
  }

  def drop(request: Request): Unit = {
    requestById.remove(request.command.msgUuid)

    requestByTimeoutById.synchronized {
      Option(requestByTimeoutById.get(request.timeoutTime)) match {
        case Some(map) =>
          map.remove(request.command.msgUuid)
          if (map.isEmpty)
            requestByTimeoutById.remove(request.timeoutTime)
        case _ =>
      }
    }
  }

  def swap(out: Request, in: Request): Unit = {
    require(in.command.msgUuid == out.command.msgUuid)
    drop(out)
    hold(in)
  }

  /**
    * Transmit some arbitrary work to the paxos cluster. Assumes that the paxos cluster has been configured with a handler
    * that can deseralize and run this work if it is chosen as the next value by the paxos algorithm.
    *
    * @param work
    * @return
    */
  def sendToCluster(work: Array[Byte]): Future[ServerResponse] = {
    val commandValue = ClientCommandValue(nextMessageUuid(), work)
    val promise: Promise[ServerResponse] = Promise()
    val request = Request(commandValue, promise, Platform.currentTime + timeoutMillis, maxAttempts, notLeaderCounter)
    transmitToCluster(notLeaderCounter, request.command)
    hold(request)
    promise.future
  }

  /**
    * Abstract method which transmits to the paxos cluster. Coulbe be implimented as TCP or UDP or even UDT.
    *
    * @param notLeaderCounter A value which is incremented whenever we learn that the node we are sending to is not the leader.
    *                         It is anticipated that a leaderCursor%cluterSize could be used to pick the node in the cluster to
    *                         transmit to in the hope that it is currently the leader.
    * @param command          The command from the client.
    */
  def transmitToCluster(notLeaderCounter: Int, command: CommandValue): Unit

  def receiveFromCluster(response: ServerResponse): Unit = {
    if (log.isDebugEnabled) log.debug("slot {} with {} found is {} is in map {}", response.logIndex, response.clientMsgId, requestById.contains(response.clientMsgId), requestById)
    requestById.get(response.clientMsgId) foreach {
      case request@Request(_, promise, _, _, _) =>
        promise.complete(Success(response))
        //log.debug("response {} for {} is {}", cid, client, response)
        drop(request)
    }
  }
}

object PaxosClusterClient {
  /**
    * You should consider overriding this to have a more stable client to server protocol.
    *
    * @param work Something to send to the paxos cluster. We assumes that you have written a custom handler somewhere out there that knows how to run deserialize and run your work.
    * @return A binary representation of your command to be transmitted on the wire and made durable by the paxos cluster.
    */
  def serialize(work: Serializable): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(work)
    oos.close
    baos.toByteArray
  }

  def cmdToByteBuf(command: CommandValue): ByteBuf = {
    import ByteBufUtils._
    Pickle.pack(command)
  }

  def byteBufToCmd(buffer: ByteBuf): CommandValue = {
    import ByteBufUtils._
    Pickle.unpack(buffer.iterator).asInstanceOf[CommandValue]
  }
}
