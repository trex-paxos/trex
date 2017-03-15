package com.github.trex_paxos.core

import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap

import com.github.trex_paxos.PaxosProperties
import com.github.trex_paxos.library._
import com.github.trex_paxos.util.Pickle
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.DatagramPacket
import io.netty.channel.socket.nio.NioDatagramChannel

import scala.util.{Failure, Success, Try}

/**
  *
  * @param paxosProperties   The paxos library configuration such as leader timeout min/max.
  * @param journal           The paxos durable store.
  * @param initialAgent      The initial state of the paxos engine at startup
  * @param deliverMembership The callback when new cluster membership is known to be chosen.
  * @param deliverClient     The callback when a new client value is known to be chosen.
  * @param clusterAddresses  The callback to get the current set of peer nodes to transmit messages to.
  */
abstract class NettyServer(val paxosProperties: PaxosProperties,
                           val journal: Journal,
                           val initialAgent: PaxosAgent,
                           val deliverMembership: PartialFunction[Payload, Any],
                           val deliverClient: PartialFunction[Payload, AnyRef],
                           clusterAddresses: => Iterable[InetSocketAddress]
                          )
  extends SimpleChannelInboundHandler[DatagramPacket] with PaxosEngine {

  val nodeUniqueId = initialAgent.nodeUniqueId

  case class ContextAndSocket(ctx: ChannelHandlerContext, inetSocketAddress: InetSocketAddress)

  case class ContextAndSocketAndMsgUUid(msgUUid: String, ctx: ChannelHandlerContext, inetSocketAddress: InetSocketAddress)

  import collection.JavaConverters._

  val clientMsgUuidToSocket: scala.collection.concurrent.Map[String, ContextAndSocket] =
    (new ConcurrentHashMap[String, ContextAndSocket]()).asScala

  val paxosIdToSocketAndMsgUuid: scala.collection.concurrent.Map[Identifier, ContextAndSocketAndMsgUUid] =
    (new ConcurrentHashMap[Identifier, ContextAndSocketAndMsgUUid]()).asScala

  @throws[Exception]
  def channelRead0(ctx: ChannelHandlerContext, packet: DatagramPacket) {
    import ByteBufUtils._
    import scala.concurrent.ExecutionContext.Implicits.global

    val paxosMessage = Pickle.unpack(packet.content().iterator).asInstanceOf[PaxosMessage]

    // if we have received a client message stash the details so that we can reply later
    paxosMessage match {
      case ClientCommandValue(msgUuid, _) =>
        clientMsgUuidToSocket.put(msgUuid, ContextAndSocket(ctx, packet.sender()))
      case _ =>
    }

    // run the algorithm on this message and send any responses as necessary
    this ? paxosMessage map { msgs =>
      transmitMessages(msgs)
    }
  }

  private def transmitMessages(msgs: Seq[PaxosMessage]) = {
    import ByteBufUtils._
    logger.info("got {}", msgs)
    msgs foreach { msg =>
      val buf: ByteBuf = Pickle.pack(msg)
      msg match {
        case m@(_: RetransmitRequest | _: RetransmitResponse | _: AcceptResponse | _: PrepareResponse | _: NotLeader) =>
          //ctx.write(new DatagramPacket(buf, packet.sender))
        case _ =>
          clusterAddresses foreach { address =>
            //ctx.write(new DatagramPacket(buf, address))
          }
      }
    }
  }

  /**
    * upgrade our knowledge of client who sent the command with the paxos identifer so that we can respond to the client when paxos has selected their command.
    *
    * @param value The command to associate with a message identifer.
    * @param id    The message identifier
    */
  override def associate(value: CommandValue, id: Identifier): Unit = {
    clientMsgUuidToSocket.get(value.msgUuid) match {
      case Some(ContextAndSocket(ctx, inetSocketAddress)) =>
        paxosIdToSocketAndMsgUuid.put(id, ContextAndSocketAndMsgUUid(value.msgUuid, ctx, inetSocketAddress))
        clientMsgUuidToSocket.remove(value.msgUuid)
      case _ =>
        logger.warning("associate called for {} and {} when no corresponding value in clientMsgUuidToSocket of size {}.", value, id, clientMsgUuidToSocket.size)
    }
  }

  def serialize(o: Any): Try[Array[Byte]]

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

  override def channelReadComplete(ctx: ChannelHandlerContext) {
    ctx.flush
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace()
    // We don't close the channel because we can keep serving requests.
  }

  /**
    * The deliver method is called when a command is committed after having been selected by consensus.
    *
    * @param payload The selected value and a delivery id that can be used to deduplicate deliveries during crash recovery.
    * @return The response to the value command that has been delivered. May be an empty array.
    */
  def deliver(payload: Payload): Any = (deliverClient orElse deliverMembership) (payload)

  // kick off the timeout timer
  logger.info("starting random timeout with first delay {}", scheduleRandomCheckTimeout())
}

object NettyServer {
  def run(port: Int, nettyServer: NettyServer): Unit = {
    val group: EventLoopGroup = new NioEventLoopGroup
    try {
      val b: Bootstrap = new Bootstrap
      b.group(group).channel(classOf[NioDatagramChannel]).option[java.lang.Boolean](ChannelOption.SO_BROADCAST, true).handler(nettyServer)
      b.bind(port).sync.channel.closeFuture.await
    }
    finally group.shutdownGracefully
  }
}