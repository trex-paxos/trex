package com.github.trex_paxos.netty

import java.util.concurrent.TimeUnit

import com.codahale.metrics.{JmxReporter, MetricRegistry}
import io.netty.bootstrap.Bootstrap
import io.netty.channel._
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import org.slf4j.LoggerFactory

object Client {
  val registry = new MetricRegistry()
  JmxReporter.forRegistry(registry).build().start()
}

class Client(val node: Node, channelHandler: Option[ChannelHandler] = None) {
  val logger = LoggerFactory.getLogger(this.getClass)

  class LoggingChannelHandler extends ChannelInboundHandlerAdapter {
    override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
      logger.debug("received {}", msg)
    }
  }

  class DisconnectChannelHandler(client: Client) extends ChannelInboundHandlerAdapter {
    override def channelInactive(ctx: ChannelHandlerContext): Unit = {
      logger.info("disconnected from {}", node)
      connectCounter.dec()
      client.connect(ctx.channel().eventLoop())
    }
  }

  private[this] var channel: Option[Channel] = None

  val connectCounter = Client.registry.counter(s"connect.hits.${node.nodeUniqueId}")
  val noConnectCounter = Client.registry.counter(s"connects.miss.${node.nodeUniqueId}")

  class ConnectionListener(val client: Client) extends ChannelFutureListener {
    override def operationComplete(channelFuture: ChannelFuture): Unit = {
      if (channelFuture.isSuccess()) {
        channel = Some(channelFuture.channel())
        connectCounter.inc()
        logger.info("Connected to {}", node)
      } else {
        logger.debug("Failed to connect to {}", node)
        val loop: EventLoop = channelFuture.channel().eventLoop()
        loop.schedule(new Runnable() {
          override
          def run() {
            client.connect(loop)
          }
        }, 1L, TimeUnit.SECONDS)
        noConnectCounter.inc()
      }
    }
  }

  def connect(loop: EventLoopGroup) = {
    val b = new Bootstrap()
    b.group(loop)
      .channel(classOf[NioSocketChannel])
      .option[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
      .handler(new ChannelInitializer[SocketChannel]() {
        override
        def initChannel(ch: SocketChannel) {
          val p = ch.pipeline()
          val h = channelHandler.getOrElse(new LoggingChannelHandler)
          p.addLast(h, new DisconnectChannelHandler(Client.this))
        }
      })
    logger.debug("connecting to {}", node)
    b.connect(node.host, node.leaderPort).addListener(new ConnectionListener(this))
  }

  val sentMeter = Client.registry.meter(s"requests.sent.${node.nodeUniqueId}")
  val dropMeter = Client.registry.meter(s"requests.drop.${node.nodeUniqueId}")

  def send(msg: scala.Any): Unit = {
    channel match {
      case Some(ch) =>
        ch.writeAndFlush(msg)
        sentMeter.mark()
      case None =>
        logger.debug("dropping msg {} as not currently connected")
        dropMeter.mark()
    }
  }

}
