package com.github.trex_paxos.internals

import java.net.InetSocketAddress
import java.util.Random

import com.github.trex_paxos.library.PaxosAgent
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.Unpooled
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel._
import io.netty.channel.socket.DatagramPacket
import io.netty.channel.socket.nio.NioDatagramChannel
import io.netty.util.CharsetUtil

class PaxosUdpHandler extends SimpleChannelInboundHandler[DatagramPacket] {
  private val random: Random = new Random
  // Quotes from Mohandas K. Gandhi:
  private val quotes: Array[String] = Array("Where there is love there is life.", "First they ignore you, then they laugh at you, then they fight you, then you win.", "Be the change you want to see in the world.", "The weak can never forgive. Forgiveness is the attribute of the strong.")

  private def nextQuote: String = {
    var quoteId: Int = 0
    random synchronized {
      quoteId = random.nextInt(quotes.length)
    }
    quotes(quoteId)
  }

  @throws[Exception]
  def channelRead0(ctx: ChannelHandlerContext, packet: DatagramPacket) {
    System.err.println(packet)
    if ("QOTM?" == packet.content.toString(CharsetUtil.UTF_8)) ctx.write(new DatagramPacket(Unpooled.copiedBuffer("QOTM: " + nextQuote, CharsetUtil.UTF_8), packet.sender))
  }

  override def channelReadComplete(ctx: ChannelHandlerContext) {
    ctx.flush
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace()
    // We don't close the channel because we can keep serving requests.
  }
}

class NettyServer(udpHandler: PaxosUdpHandler, initalAgent: PaxosAgent) {
  private[this] var paxosAgent: PaxosAgent = initalAgent
}

object NettyServer {
  private val PORT: Int = System.getProperty("port", "7686").toInt
  def main(args: Array[String]): Unit = {
    val group: EventLoopGroup = new NioEventLoopGroup
    try {
      val b: Bootstrap = new Bootstrap
      b.group(group).channel(classOf[NioDatagramChannel]).option[java.lang.Boolean](ChannelOption.SO_BROADCAST, true).handler(new PaxosUdpHandler)
      b.bind(PORT).sync.channel.closeFuture.await
    }
    finally group.shutdownGracefully
  }
}

class QuoteOfTheMomentClientHandler extends SimpleChannelInboundHandler[DatagramPacket] {
  @throws[Exception]
  def channelRead0(ctx: ChannelHandlerContext, msg: DatagramPacket) {
    val response: String = msg.content.toString(CharsetUtil.UTF_8)
    if (response.startsWith("QOTM: ")) {
      System.out.println("Quote of the Moment: " + response.substring(6))
      ctx.close
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace()
    ctx.close
  }
}

object QuoteOfTheMomentClient {
  private val PORT: Int = System.getProperty("port", "7686").toInt

  @throws[Exception]
  def main(args: Array[String]) {
    val group: EventLoopGroup = new NioEventLoopGroup
    try {
      val b: Bootstrap = new Bootstrap
      b.group(group).channel(classOf[NioDatagramChannel]).option[java.lang.Boolean](ChannelOption.SO_BROADCAST, true).handler(new QuoteOfTheMomentClientHandler)
      val ch: Channel = b.bind(0).sync.channel
      // Broadcast the QOTM request to port 8080.
      ch.writeAndFlush(new DatagramPacket(Unpooled.copiedBuffer("QOTM?", CharsetUtil.UTF_8), new InetSocketAddress("255.255.255.255", PORT))).sync
      // QuoteOfTheMomentClientHandler will close the DatagramChannel when a
      // response is received.  If the channel is not closed within 5 seconds,
      // print an error message and quit.
      if (!ch.closeFuture.await(5000)) System.err.println("QOTM request timed out.")
    }
    finally group.shutdownGracefully
  }
}