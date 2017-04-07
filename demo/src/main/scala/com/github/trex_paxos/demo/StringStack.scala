package com.github.trex_paxos.demo

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import com.github.trex_paxos.core._
import com.github.trex_paxos.javademo.{StringStack, StringStackImpl}
import com.github.trex_paxos.library._
import com.github.trex_paxos._
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration

object StringStackShared {
  val quorum = Quorum(2, Set(Weight(2552,1), Weight(2562,1), Weight(2572,1)))
  val membership = Membership(quorum, quorum, Set(
      Node(2552, Addresses(Address("localhost", 2552), Address("localhost", 2553)) )
      , Node(2562, Addresses(Address("localhost", 2562), Address("localhost", 2563)) )
      , Node(2572, Addresses(Address("localhost", 2572), Address("localhost", 2573)) )

    ), Some(0))

  val LocationsRegex = """([^:]*):([0-9]*)\|([^:]*):([0-9]*)""".r
  val UTF8 = "UTF8"

  def doStackWork(stack: StringStack, input: String): Option[String] = {
    if (input.startsWith("push ")) {
      Option(stack.push(input.substring("push ".length)))
    }
    else if (input.startsWith("pop")) if (stack.empty) None
    else Option(stack.pop)
    else if (input.startsWith("peek")) if (stack.empty) None
    else Option(stack.peek)
    else None
  }

}

object StringStackServer {

  import StringStackShared._

  val PushPattern = "push[\\s]*([^\\s]*)".r

  def main(args: Array[String]): Unit = {
    if (args.length != 2) usage(1)

    val configName: String = args(0)
    val nodeUniqueId: Integer = Integer.valueOf(args(1))

    val stack: StringStack = new StringStackImpl(new java.io.File(System.getProperty("java.io.tmpdir") + "/stack" + nodeUniqueId.toString))

    val folder = new java.io.File(System.getProperty("java.io.tmpdir") + "/" + nodeUniqueId)

    if (!folder.exists) {
      folder.mkdirs()
    }

    if (!folder.exists || !folder.canRead || !folder.canWrite) {
      System.err.println(folder.getCanonicalPath + " does not exist or do not have permission to read and write. Exiting.")
      System.exit(-1)
    }

    val journal: MapDBStore = new MapDBStore(new java.io.File(folder, "journal"), Int.MaxValue)
    val progress = journal.loadProgress()
    val initialAgent = PaxosAgent(nodeUniqueId, Follower, PaxosData(progress, 50, 3000), new DefaultQuorumStrategy(() => 3))
    val deliverMembership: PartialFunction[Payload, Array[Byte]] = {
      case p =>
        System.err.println("not implimented")
        Array[Byte]()
    }
    val deliverClient: PartialFunction[Payload, Array[Byte]] = {
      case Payload(index, command) => command match {
        case ClientCommandValue(msgUuid, bytes) =>
          val string = new String(bytes, UTF8)
          doStackWork(stack, string) match {
            case Some(string) => string.getBytes(UTF8)
            case None => Array[Byte]()
          }
      }
    }
//
//    def clusterAddresses: Iterable[InetSocketAddress] =
//      m.nodes.filter({ case Node(id, _) => id != nodeUniqueId }) map {
//        case Node(_, Addresses(_, )) =>
//          location match {
//            case LocationsRegex(_, _, host, port) =>
//              new InetSocketAddress(host, port.toInt)
//            case f => throw new IllegalArgumentException(s"could not parse $location")
//          }
//      }

    val addresss = ???

    def transitMessages(msg: Seq[PaxosMessage]): Unit = {

    }

    val paxosSystem = new PaxosEngine(
      PaxosProperties(1000, 3000),
      journal,
      initialAgent,
      deliverMembership,
      deliverClient,
      ByteArraySerializer.serialize _,
      transitMessages _
    ) with LogbackPaxosLogging {
      override def logger: PaxosLogging = this
    }

  }

  def usage(returned: Int) {
    System.out.println("usage:   StackClusterNode config nodeId")
    System.out.println("example: StackClusterNode server3.conf 2552")
    System.exit(returned)
  }
}

object StringStackClient {

  val logger = LoggerFactory.getLogger(classOf[PaxosLogging]);

  import StringStackShared._

  val fiveMsDuration = FiniteDuration(5, TimeUnit.MILLISECONDS)
  val fiveSecDuration = FiniteDuration(5, TimeUnit.SECONDS)

  def main(args: Array[String]): Unit = {
    if (args.length == 0) usage(1)

    var stack: StringStack = null

    if (args(0).startsWith("local")) {
      System.out.println("using local stack")
      stack = new StringStackImpl
    }
    else if (args(0).startsWith("clustered")) {
      if (args.length != 3) usage(2)
      System.out.println("using clustered stack")
      //val nettyClusterClient = driver(args(1), args(2))

      stack = new StringStack {
        override def peek(): String = sendStringCommand("peek")

        override def push(item: String): String = sendStringCommand(s"push:$item")

        override def pop(): String = sendStringCommand("pop")

        def sendStringCommand(cmd: String): String = {
          logger.info("sending command {}", cmd)
//          val f = nettyClusterClient.sendToCluster(cmd.getBytes(UTF8))
//          val r = Await.result(f, fiveSecDuration)
//          r.response match {
//            case Some(bytes) => new String(bytes, UTF8)
//            case f => throw new AssertionError(f.toString)
//          }
          ""
        }

        override def empty(): Boolean = ???


        override def search(o: scala.Any): Int = ???
      }

      //NettyClusterClient.startServer(nettyClusterClient)
    }
    else {
      stack = null
      System.err.println("neither local or clustered: " + args(0))
      System.exit(2)
    }

    try {
      val br: BufferedReader = new BufferedReader(new InputStreamReader(System.in))
      var input: String = null
      while (Option(input = br.readLine).isDefined)
        reportStackWork(stack, input)
    }
    catch {
      case io: IOException => {
        io.printStackTrace()
      }
    }
  }

  def reportStackWork(stack: StringStack, input: String): Unit = System.out.println(doStackWork(stack, input))

  def usage(returned: Int) {
    System.err.println("usage    : StringStack local|clustered [config] [hostname]")
    System.err.println("example 1: StringStack local")
    System.err.println("example 2: StringStack clustered client3.conf 127.0.0.1")
    System.exit(returned)
  }

//  def driver(configName: String, hostname: String): NettyClusterClient = {
//    new NettyClusterClient(fiveMsDuration, Int.MaxValue, StringStackShared.m) with LogbackjPaxosLogging {
//      override def log: PaxosLogging = this
//    }
//  }


}