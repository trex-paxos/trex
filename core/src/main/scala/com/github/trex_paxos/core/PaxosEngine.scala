package com.github.trex_paxos.core

import java.net.InetSocketAddress
import java.security.SecureRandom
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.github.trex_paxos.PaxosProperties
import com.github.trex_paxos.library._
import com.github.trex_paxos.util.Pickle
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.socket.DatagramPacket

import scala.collection.immutable.{Seq, SortedMap}
import scala.compat.Platform
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success, Try}

abstract class PaxosEngine(val paxosProperties: PaxosProperties,
                           val journal: Journal,
                           val initialAgent: PaxosAgent,
                           val deliverMembership: PartialFunction[Payload, Array[Byte]],
                           val deliverClient: PartialFunction[Payload, Array[Byte]],
                           val serialize: (Any) => Try[Array[Byte]],
                           val transmitMessages: Seq[PaxosMessage] => Unit
                          )
  extends PaxosIO
    with CollectingPaxosIO {

  private[this] var agent = initialAgent

  private val paxosAlgorithm = new PaxosAlgorithm

  /**
    * The deliver method is called when a command is committed after having been selected by consensus.
    *
    * @param payload The selected value and a delivery id that can be used to deduplicate deliveries during crash recovery.
    * @return The response to the value command that has been delivered. May be an empty array.
    */
  def deliver(payload: Payload): Array[Byte] = (deliverClient orElse deliverMembership) (payload)

  private[this] val singleThread = ExecutionContext.fromExecutor(java.util.concurrent.Executors.newFixedThreadPool(1))

  singleThread.execute(new Runnable {
    override def run() = Thread.currentThread().setName("PaxosEngine")
  })

  def receive(message: PaxosMessage): Future[Seq[PaxosMessage]] = Future {
    require(logger != null)
    outbound.clear()
    val oldData = agent.data
    val event = PaxosEvent(this, agent, message)
    agent = paxosAlgorithm(event)
    logger.debug("|{}|{}|{}|{}|", agent.nodeUniqueId, oldData, message, agent.data, outbound)
    collection.immutable.Seq[PaxosMessage](outbound: _*)
  } (singleThread)

  val nodeUniqueId = initialAgent.nodeUniqueId

  case class ContextAndSocket(ctx: ChannelHandlerContext, inetSocketAddress: InetSocketAddress)

  case class ContextAndSocketAndMsgUUid(msgUUid: String, ctx: ChannelHandlerContext, inetSocketAddress: InetSocketAddress)

  import collection.JavaConverters._

  val clientMsgUuidToSocket: scala.collection.concurrent.Map[String, ContextAndSocket] =
    (new ConcurrentHashMap[String, ContextAndSocket]()).asScala

  val paxosIdToSocketAndMsgUuid: scala.collection.concurrent.Map[Identifier, ContextAndSocketAndMsgUUid] =
    (new ConcurrentHashMap[Identifier, ContextAndSocketAndMsgUUid]()).asScala

  def associate(value: CommandValue, id: Identifier): Unit = {
    clientMsgUuidToSocket.get(value.msgUuid) match {
      case Some(ContextAndSocket(ctx, inetSocketAddress)) =>
        paxosIdToSocketAndMsgUuid.put(id, ContextAndSocketAndMsgUUid(value.msgUuid, ctx, inetSocketAddress))
        clientMsgUuidToSocket.remove(value.msgUuid)
      case _ =>
        logger.warning("associate called for {} and {} when no corresponding value in clientMsgUuidToSocket of size {}.", value, id, clientMsgUuidToSocket.size)
    }
  }

  override def respond(resultsOpt: Option[Map[Identifier, Any]]): Unit = resultsOpt match {
    case Some(results) =>
      results foreach {
        case (id, result) =>
          serialize(result) match {
            case Success(bytes) =>
              paxosIdToSocketAndMsgUuid.get(id) foreach {
                case ContextAndSocketAndMsgUUid(msgUUid, ctx, clientSocket) =>
                  val serverResponse = ServerResponse(id.logIndex, msgUUid, Option(bytes))
                  import ByteBufUtils._
                  val buffer: ByteBuf = Pickle.pack(serverResponse)
                  ctx.write(new DatagramPacket(buffer, clientSocket))
              }
            case Failure(ex) =>
              logger.error("exception {} attempting to serialize class {} instance {}", ex.toString, result.getClass, result)
          }
          paxosIdToSocketAndMsgUuid.remove(id)
      }

    case None => // we are no longer the leader send an  to any pending clients and clear the leader messaging state maps
      def sendLostLeaderException(contextAndSocketAndMsgUUid: ContextAndSocketAndMsgUUid): Unit = {
        import ByteBufUtils._
        val byteBuf: ByteBuf = Pickle.pack(LostLeadershipException(nodeUniqueId, contextAndSocketAndMsgUUid.msgUUid))
        contextAndSocketAndMsgUUid.ctx.write(new DatagramPacket(byteBuf, contextAndSocketAndMsgUUid.inetSocketAddress))
      }

      clientMsgUuidToSocket foreach {
        case (msgUuid, ctxAndSocket) =>
          sendLostLeaderException(ContextAndSocketAndMsgUUid(msgUuid, ctxAndSocket.ctx, ctxAndSocket.inetSocketAddress))
      }
      paxosIdToSocketAndMsgUuid foreach {
        case (_, v) => sendLostLeaderException(v)
      }
      clientMsgUuidToSocket.clear()
      paxosIdToSocketAndMsgUuid.clear()
  }

  val secureRandom = new SecureRandom()

  def random: Random = secureRandom

  def clock() = Platform.currentTime

  def randomInterval: Long = {
    val properties = paxosProperties
    val min = properties.leaderTimeoutMin
    val max = properties.leaderTimeoutMax
    val range = max - min
    require(min > 0)
    require(max > 0)
    require(range > 0)
    min + (range * random.nextDouble()).toLong
  }

  import scala.concurrent.ExecutionContext.Implicits.global

  def scheduleRandomCheckTimeout() = {
    val randomDelay = randomInterval
    DelayedFuture(FiniteDuration(randomDelay, TimeUnit.MILLISECONDS)) {
      this.receive(CheckTimeout).map {
        msgs => transmitMessages(msgs)
      }
    }
    clock() + randomDelay
  }

  scheduleRandomCheckTimeout()

  /**
    * Override this to a value that appropriately suppresses follower timeouts
    */
  def heartbeatInterval = Math.max(paxosProperties.leaderTimeoutMin / 3, 1)

  {
    DelayedFuture( paxosProperties.leaderTimeoutMin, heartbeatInterval ){
      this.receive(HeartBeat).map {
        msgs => transmitMessages(msgs)
      }
    }
  }
}

object PaxosEngine {
  def initialAgent(nodeUniqueId: Int, progress: Progress, clusterSize: () => Int) = new PaxosAgent(nodeUniqueId, Follower, PaxosData(progress, 0, 0,
    SortedMap.empty[Identifier, scala.collection.immutable.Map[Int, PrepareResponse]](Ordering.IdentifierLogOrdering), None,
    SortedMap.empty[Identifier, AcceptResponsesAndTimeout](Ordering.IdentifierLogOrdering)
  ), DefaultQuorumStrategy(clusterSize))
}
