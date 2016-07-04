package com.github.trex_paxos.netty4

import java.net.InetSocketAddress

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.Unpooled
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.DatagramPacket
import io.netty.channel.socket.nio.NioDatagramChannel
import io.netty.util.CharsetUtil

object NettyUdpClient {
  val PORT: Int = System.getProperty("port", "7686").toInt

  def main(args: Array[String]) {
    val group: EventLoopGroup = new NioEventLoopGroup()
    try {
      val b = new Bootstrap()
      b.group(group)
        .channel(classOf[NioDatagramChannel])
        .option(ChannelOption.SO_BROADCAST, java.lang.Boolean.TRUE)
        .handler(new QuoteOfTheMomentClientHandler())

      val ch: Channel = b.bind(0).sync().channel()

      // Broadcast the QOTM request to port 8080.
      ch.writeAndFlush(new DatagramPacket(
        Unpooled.copiedBuffer("QOTM?", CharsetUtil.UTF_8),
        new InetSocketAddress("255.255.255.255", PORT))).sync()

      // QuoteOfTheMomentClientHandler will close the DatagramChannel when a
      // response is received.  If the channel is not closed within 5 seconds,
      // print an error message and quit.
      if (!ch.closeFuture().await(5000)) {
        System.err.println("QOTM request timed out.")
      }
    } finally {
      group.shutdownGracefully()
    }
  }
}

class QuoteOfTheMomentClientHandler extends SimpleChannelInboundHandler[DatagramPacket] {

  override
  def channelRead0(ctx: ChannelHandlerContext, msg: DatagramPacket) {
    val response: String = msg.content().toString(CharsetUtil.UTF_8)
    if (response.startsWith("QOTM: ")) {
      System.out.println("Quote of the Moment: " + response.substring(6))
      ctx.close()
    }
  }

  override
  def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace()
    ctx.close()
  }
}