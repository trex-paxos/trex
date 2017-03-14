package com.github.trex_paxos.netty

import com.github.trex_paxos.library.{BallotNumber, Identifier, Prepare}
import com.github.trex_paxos.util.Pickle
import io.netty.channel.nio.NioEventLoopGroup

object TestClient {
  def main(args: Array[String]): Unit = {
    val client = new TcpClient(Node(99, "localhost", 8007, 8007))
    client.connect(new NioEventLoopGroup())
    val p = Prepare(Identifier(1, BallotNumber(2, 3), 4L))
    while(true){
      client.send(TcpShared.marshal(Pickle.pack(p)))
      Thread.sleep(1000L)
    }
  }

}
