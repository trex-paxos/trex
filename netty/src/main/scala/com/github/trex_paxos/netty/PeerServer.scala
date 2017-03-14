package com.github.trex_paxos.netty

import com.github.trex_paxos.library.{BallotNumber, Identifier, Prepare}
import com.github.trex_paxos.util.{ByteChain, Pickle}
import io.netty.bootstrap.{Bootstrap, ServerBootstrap}
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.{NioServerSocketChannel, NioSocketChannel}

case class Node(nodeUniqueId: Int, host: String, leaderPort: Int, peerPort: Int)

object TcpShared {

  val nodes = Seq(Node(1, "localhost", 1110, 1111),
    Node(2, "localhost", 1220, 1221),
    Node(3, "localhost", 1330, 1331))

  def iterator(in: ByteBuf) = {
    new Iterator[Byte] {
      override def hasNext: Boolean = in.isReadable()

      override def next(): Byte = in.readByte()
    }
  }

  def marshal(out: ByteChain): ByteBuf = {
    val bb = Unpooled.buffer(out.length)
    out.bytes.foreach(bb.writeBytes(_))
    bb
  }
}

object TcpServer {

  class LeaderHandler(nodeUniqueId: Int) extends ChannelInboundHandlerAdapter {
    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
      cause.printStackTrace()
      ctx.close()
    }

    override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
      val in = msg.asInstanceOf[ByteBuf]
      val m = Pickle.unpack(TcpShared.iterator(in))
      System.out.println(s"leader nodeUniqueId: $nodeUniqueId sees $m")
      in.release()
    }
  }

  class PeerHandler(nodeUniqueId: Int) extends ChannelInboundHandlerAdapter {
    override def channelRead(ctx: ChannelHandlerContext, msg: Object) {
      val in = msg.asInstanceOf[ByteBuf]
      val m = Pickle.unpack(TcpShared.iterator(in))
      System.out.println(s"leader nodeUniqueId: $nodeUniqueId sees $m")
      in.release()
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
      cause.printStackTrace()
      ctx.close()
    }
  }

  def runServer(nodes: Seq[Node], node: Node): Unit = {
    System.out.println(s"starting $node")
    def launch(port: Int, boss: NioEventLoopGroup, worker: NioEventLoopGroup, handler: ChannelInboundHandlerAdapter) = {
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
      leaderBootstrap.bind(port)
        .sync()
    }

    val leaderBossGroup = new NioEventLoopGroup()
    val leaderWorkerGroup = new NioEventLoopGroup()
    val peerBossGroup = new NioEventLoopGroup()
    val peerWorkerGroup = new NioEventLoopGroup()
    try {
      val peerHandler = new PeerHandler(node.nodeUniqueId)
      launch(node.peerPort, peerBossGroup, peerWorkerGroup, peerHandler)
      val leaderHandler = new LeaderHandler(node.nodeUniqueId)
      val leaderFuture = launch(node.leaderPort, leaderBossGroup, leaderWorkerGroup, leaderHandler)
      leaderFuture.channel().closeFuture().sync()
    } finally {
      leaderBossGroup.shutdownGracefully()
      leaderWorkerGroup.shutdownGracefully()
      peerBossGroup.shutdownGracefully()
      peerWorkerGroup.shutdownGracefully()
    }
  }

  def main(args: Array[String]): Unit = {
    val node = args.toSeq.headOption match {
      case Some("1") => TcpShared.nodes(0)
      case Some("2") => TcpShared.nodes(1)
      case Some("3") => TcpShared.nodes(2)
      case _ => throw new IllegalArgumentException("must pass a number between 1 and 3 inclusive")
    }
    runServer(TcpShared.nodes, node)
  }
}
// TODO abstract this out to a component which keeps on trying to send until told to stop
// recall that sending is async on netty.
object Driver {

  class DriverHandler extends ChannelInboundHandlerAdapter {
    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
      cause.printStackTrace()
      ctx.close()
    }

    override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
      val in = msg.asInstanceOf[ByteBuf]
      val m = Pickle.unpack(TcpShared.iterator(in))
      System.out.println(s"client received back: $m")
      in.release()
    }
  }

  def main(args: Array[String]) {
    val node = args.toSeq.headOption match {
      case Some("1") => TcpShared.nodes(0)
      case Some("2") => TcpShared.nodes(1)
      case Some("3") => TcpShared.nodes(2)
      case _ => throw new IllegalArgumentException("must pass a number between 1 and 3 inclusive")
    }
    System.out.println(s"sending to node $node")
    val group = new NioEventLoopGroup()
    try {
      def doConnect(): Unit = {
        val b = new Bootstrap()
        b.group(group)
          .channel(classOf[NioSocketChannel])
          .option[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
          .handler(new ChannelInitializer[SocketChannel]() {
            override
            def initChannel(ch: SocketChannel) {
              val p = ch.pipeline()
              p.addLast(new DriverHandler())
            }
          })
        b.connect(node.host, node.leaderPort).addListener(new ChannelFutureListener(){
          override def operationComplete(future: ChannelFuture): Unit = {
            if( !future.isSuccess ) doConnect()
            else {
              // TODO start a read loop here?
              System.out.println("p1 or p2?")
              val line = scala.io.StdIn.readLine()
              val p = line match {
                case "p1" => Option(Prepare(Identifier(1, BallotNumber(2, 3), 4L)))
                case "p2" => Option(Prepare(Identifier(5, BallotNumber(6, 7), 8L)))
                case _ => System.out.println("pardon?")
                  None
              }
              if( p.isDefined ) future.channel().write(TcpShared.marshal(Pickle.pack(p.get)))

            }
          }
        })
      }
      doConnect()
      // TODO await some latch
      Thread.sleep(Int.MaxValue)
    } finally {
      group.shutdownGracefully()
    }
  }
}