package com.github.trex_paxos.netty

import com.github.trex_paxos.library.{BallotNumber, Identifier, Prepare}
import com.github.trex_paxos.util.Pickle
import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.channel.nio.NioEventLoopGroup
import org.slf4j.LoggerFactory

object TestClient {
  val logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val handler = new ChannelInboundHandlerAdapter {
      override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
        val m = msg match {
          case in: ByteBuf =>
            try {
              Pickle.unpack(ByteBufIterable(in).iterator)
            } finally {
              in.release()
            }
        }
        logger.info("{}", m)
      }
    }
    val client = new Client(TestServer.nodes(0))
    client.connect(new NioEventLoopGroup())
    var count = 0
    while(true){
      val p = Prepare(Identifier(count, BallotNumber(2, 3), 4L))
      count = count + 1
      client.send(ByteBufUtils.byteChainToByteBuf(Pickle.pack(p)))
      Thread.sleep(1000L)
    }
  }

}
