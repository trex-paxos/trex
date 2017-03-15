package com.github.trex_paxos.netty4

import java.io.{BufferedReader, InputStreamReader}

import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel._
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import io.netty.handler.codec.{DelimiterBasedFrameDecoder, Delimiters}

/**
  * Handles a client-side channel.
  */
@Sharable class TelnetClientHandler extends SimpleChannelInboundHandler[String] {
  @throws[Exception]
  protected def channelRead0(ctx: ChannelHandlerContext, msg: String) {
    System.err.println(msg)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace()
    ctx.close
  }
}

/**
  * Creates a newly configured {@link ChannelPipeline} for a new channel.
  */
object TelnetClientInitializer {
  private val DECODER = new StringDecoder
  private val ENCODER = new StringEncoder
  private val CLIENT_HANDLER = new TelnetClientHandler
}

class TelnetClientInitializer() extends ChannelInitializer[SocketChannel] {
  def initChannel(ch: SocketChannel) {
    val pipeline = ch.pipeline
    // Add the text line codec combination first,
    pipeline.addLast(new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter: _*))
    pipeline.addLast(TelnetClientInitializer.DECODER)
    pipeline.addLast(TelnetClientInitializer.ENCODER)
    // and then business logic.
    pipeline.addLast(TelnetClientInitializer.CLIENT_HANDLER)
  }
}

/**
  * Simplistic telnet client.
  */
object TelnetClient {
  val HOST = System.getProperty("host", "127.0.0.1")
  val PORT1 = System.getProperty("port", "2552").toInt
  val PORT2 = System.getProperty("port", "2553").toInt

  @throws[Exception]
  def main(args: Array[String]) {
    // Configure SSL.
    val group = new NioEventLoopGroup
    try{
      val b = new Bootstrap
      b.group(group).channel(classOf[NioSocketChannel]).handler(new TelnetClientInitializer())
      // Start the connection attempt.
      val ch: Channel = b.connect(HOST, PORT1).sync.channel
      val ch2: Channel = b.connect(HOST, PORT2).sync.channel
      // Read commands from the stdin.
      var lastWriteFuture: (ChannelFuture, ChannelFuture) = null
      val in = new BufferedReader(new InputStreamReader(System.in))
      import scala.util.control.Breaks._
      breakable {
        while (true) {
          val line = in.readLine
          if (line == null) break
          // Sends the received line to the server.
          lastWriteFuture = (ch.writeAndFlush(s"1>$line\r\n"), ch2.writeAndFlush(s"2>$line\r\n"))
          // If user typed the 'bye' command, wait until the server closes
          // the connection.
          if ("bye" == line.toLowerCase) {
            break
          }
        }
      }
      // Wait until all messages are flushed before closing the channel.
      if (lastWriteFuture != null) lastWriteFuture match {
        case (a, b) =>
          a.sync
          b.sync
      }
    }

    finally group.shutdownGracefully()
  }
}
