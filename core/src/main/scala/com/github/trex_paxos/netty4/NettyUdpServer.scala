package com.github.trex_paxos.netty4

import java.util.Random

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.Unpooled
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.DatagramPacket
import io.netty.channel.socket.nio.NioDatagramChannel
import io.netty.channel.{ChannelHandlerContext, ChannelOption, EventLoopGroup, SimpleChannelInboundHandler}
import io.netty.util.CharsetUtil

object QuoteOfTheMomentServerHandler {
  private val random: Random = new Random
  private val quotes: Array[String] = Array("Where there is love there is life.", "First they ignore you, then they laugh at you, then they fight you, then you win.", "Be the change you want to see in the world.", "The weak can never forgive. Forgiveness is the attribute of the strong.")

  private def nextQuote: String = {
    var quoteId: Int = 0
    random synchronized {
      quoteId = random.nextInt(quotes.length)
    }
    return quotes(quoteId)
  }
}

class QuoteOfTheMomentServerHandler extends SimpleChannelInboundHandler[DatagramPacket] {
  @throws[Exception]
  def channelRead0(ctx: ChannelHandlerContext, packet: DatagramPacket) {
    System.err.println(packet)
    if ("QOTM?" == packet.content.toString(CharsetUtil.UTF_8)) {
      ctx.write(new DatagramPacket(Unpooled.copiedBuffer("QOTM: " + QuoteOfTheMomentServerHandler.nextQuote, CharsetUtil.UTF_8), packet.sender))
    }
  }

  override def channelReadComplete(ctx: ChannelHandlerContext) {
    ctx.flush
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace
  }

}

object NettyUdpServer {

  val PORT = System.getProperty("port", "7686").toInt;
  def main(args: Array[String]): Unit = {
    val group: EventLoopGroup = new NioEventLoopGroup
    try {
      val b: Bootstrap = new Bootstrap
      b.group(group).channel(classOf[NioDatagramChannel]).option(ChannelOption.SO_BROADCAST, java.lang.Boolean.TRUE).handler(new QuoteOfTheMomentServerHandler)
      b.bind(PORT).sync.channel.closeFuture.await
    } finally {
      group.shutdownGracefully
    }
  }
}
