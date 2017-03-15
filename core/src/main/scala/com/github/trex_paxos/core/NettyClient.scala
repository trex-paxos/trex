package com.github.trex_paxos.core

import java.net.InetSocketAddress

import com.github.trex_paxos.Membership
import com.github.trex_paxos.library.{CommandValue, PaxosLogging, ServerResponse}
import com.github.trex_paxos.util.Pickle
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.DatagramPacket
import io.netty.channel.socket.nio.NioDatagramChannel

import scala.concurrent.duration.Duration

object NettyClusterClient {

  @throws[Exception]
  def startServer(nettyClusterClient: NettyClusterClient) = {
    val group: EventLoopGroup = new NioEventLoopGroup
    val b: Bootstrap = new Bootstrap
    b.group(group).channel(classOf[NioDatagramChannel]).option[java.lang.Boolean](ChannelOption.SO_BROADCAST, true).handler(nettyClusterClient)
    val ch: Channel = b.bind(0).sync.channel
    nettyClusterClient.channel = Some(ch)
    group
  }
}

abstract class NettyClusterClient(val requestTimeout: Duration, val maxAttempts: Int, membership: => Membership)
  extends SimpleChannelInboundHandler[DatagramPacket] with PaxosClusterClient {

  // TODO remove annoying var
  var channel: Option[Channel] = None

  @throws[Exception]
  def channelRead0(ctx: ChannelHandlerContext, msg: DatagramPacket) {
    import ByteBufUtils._
    val serverResponse = Pickle.unpack(msg.content().iterator).asInstanceOf[ServerResponse]
    this.receiveFromCluster(serverResponse)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace()
    ctx.close
  }

  def membersToInetSocketAddress(membership: Membership) = {
    (membership.locations.values map {
      case location =>
        // "127.0.0.1:2552|127.0.0.1:2553"
        val locationParts: Array[String] = location.split("[|]")
        val clientLocationParts: Array[String] = locationParts(0).split("[:]")
        new InetSocketAddress(clientLocationParts(0), clientLocationParts(1).toInt)
    }).toSeq
  }

  val sockets: Seq[InetSocketAddress] = membersToInetSocketAddress(membership)

  import PaxosClusterClient._

  override def transmitToCluster(notLeaderCounter: Int, command: CommandValue): Unit = {
    channel match {
      case None => System.err.println("no channel :-(")
      case Some(channel) =>
        val buffer: ByteBuf = cmdToByteBuf(command)
        val socket = sockets(notLeaderCounter % sockets.size)
        channel.writeAndFlush(new DatagramPacket(buffer, socket)).sync
    }
  }

}
