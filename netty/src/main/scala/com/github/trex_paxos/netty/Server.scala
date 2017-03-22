package com.github.trex_paxos.netty

import com.github.trex_paxos.core.PaxosEngine
import com.github.trex_paxos.Node
import com.github.trex_paxos.util.Pickle
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

object Server {

  val logger = LoggerFactory.getLogger(this.getClass)

  class ClientHandler(nodeUniqueId: Int) extends ChannelInboundHandlerAdapter {

    val clientRequests = JmxMetricsRegistry.registry.meter(s"requests.client.in.${nodeUniqueId}")

    override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
      val m = msg match {
        case in: ByteBuf =>
          Pickle.unpack(ByteBufIterable(in).iterator)
          in.release()
      }
      clientRequests.mark()
      logger.trace(s"leader nodeUniqueId: $nodeUniqueId sees $m")
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
      cause match {
        case NonFatal(e) =>
          logger.error(e.toString, e)
      }
      ctx.close()
    }
  }


  class PeerHandler(nodeUniqueId: Int) extends ChannelInboundHandlerAdapter {
    val peerRequests = JmxMetricsRegistry.registry.meter(s"requests.peers.in.${nodeUniqueId}")

    override def channelRead(ctx: ChannelHandlerContext, msg: Object) {
      val m = msg match {
        case in: ByteBuf =>
          Pickle.unpack(ByteBufIterable(in).iterator)
          in.release()
      }
      peerRequests.mark()
      logger.trace(s"leader nodeUniqueId: $nodeUniqueId sees $m")
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
      cause match {
        case NonFatal(e) =>
          logger.error(e.toString, e)
      }
      ctx.close()
    }
  }

  def runServer(initialNodes: Seq[Node], node: Node, paxosEngine: PaxosEngine): Unit = {
    logger.info(s"starting $node")

    def launch(inetHost: String, port: Int, boss: NioEventLoopGroup, worker: NioEventLoopGroup, handler: ChannelInboundHandlerAdapter) = {
      val leaderBootstrap = new ServerBootstrap()
      leaderBootstrap.group(boss, worker)
        .channel(classOf[NioServerSocketChannel])
        .childHandler(new ChannelInitializer[SocketChannel]() {
          def initChannel(ch: SocketChannel): Unit = {
            ch.pipeline().addLast(handler)
          }
        })
        .option[java.lang.Integer](ChannelOption.SO_BACKLOG, 128)
        .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
      leaderBootstrap.bind(inetHost, port)
        .sync()
    }

    val leaderBossGroup = new NioEventLoopGroup()
    val leaderWorkerGroup = new NioEventLoopGroup()
    val peerBossGroup = new NioEventLoopGroup()
    val peerWorkerGroup = new NioEventLoopGroup()
    try {
      val peerHandler = new PeerHandler(node.nodeIdentifier)
      launch(node.peerAddressHostname, node.peerAddressPort, peerBossGroup, peerWorkerGroup, peerHandler)
      val leaderHandler = new ClientHandler(node.nodeIdentifier)
      val leaderFuture = launch(node.leaderAddressHostname, node.leaderAddressPort, leaderBossGroup, leaderWorkerGroup, leaderHandler)
      leaderFuture.channel().closeFuture().sync()
      logger.info("leader channel has closed")
    } catch {
      case NonFatal(e) =>
        logger.error(e.toString, e)
    } finally {
      leaderBossGroup.shutdownGracefully()
      leaderWorkerGroup.shutdownGracefully()
      peerBossGroup.shutdownGracefully()
      peerWorkerGroup.shutdownGracefully()
    }
  }
}
