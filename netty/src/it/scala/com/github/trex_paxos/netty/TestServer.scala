package com.github.trex_paxos.netty

import com.github.trex_paxos._
import com.github.trex_paxos.core.{ByteArraySerializer, MapDBStore, PaxosEngine}
import com.github.trex_paxos.library._
import org.slf4j.LoggerFactory

import scala.collection.immutable.Seq

object TestServer {

  val logger = LoggerFactory.getLogger(this.getClass)

  val nodes = Seq(Node(1, Addresses(Address("localhost", 1110), Address("localhost",1111))),
    Node(2, Addresses(Address("localhost", 1220), Address("localhost",1221))),
      Node(3, Addresses(Address("localhost", 1330), Address("localhost",1331))))

  def main(args: Array[String]): Unit = {
    val node = args.toSeq.headOption match {
      case Some("1") => nodes(0)
      case Some("2") => nodes(1)
      case Some("3") => nodes(2)
      case _ => throw new IllegalArgumentException("must pass a number between 1 and 3 inclusive")
    }

    def nodeIdentifier = node.nodeIdentifier

    val journal: MapDBStore = tempFolderJournal(node.nodeIdentifier)

    if( journal.loadMembership().isEmpty ) {
      val quorum = Quorum(2, Set(Weight(1,1), Weight(2,1), Weight(3,1)))
      val membership = Membership(0L, quorum, quorum, nodes.toSet)

      journal.saveMembership(0, membership)
    }

    val initialProgress = journal.loadProgress()
    logger.info("initialProgress: {}", initialProgress)

    val initialAgent = PaxosAgent(nodeIdentifier, Follower, PaxosData(initialProgress, 50, 3000), new DefaultQuorumStrategy(() => 3))

    // echo the response back
    val deliverClient: PartialFunction[Payload, AnyRef] = {
      case Payload(_, ClientCommandValue(msgUuid, bytes)) =>
        val echo= new String(bytes)
        logger.info("deliver client {} got {}", msgUuid: Any, echo)
        echo
    }

    val clusterDriver = new ClusterDriver(nodeIdentifier, journal, ByteArraySerializer.deserialize _)

    val paxosEngine = new PaxosEngine(
      PaxosProperties(1000, 3000),
      journal,
      initialAgent,
      clusterDriver.deliverMembership,
      deliverClient,
      ByteArraySerializer.serialize _,
      clusterDriver.transmitMessages _
    ) with LogbackPaxosLogging {
      override def logger: PaxosLogging = this
    }

    Server.runServer(TestServer.nodes, node, paxosEngine)
  }

  def tempFolderJournal(nodeIdentifier: Int) = {
    val folder = new java.io.File(System.getProperty("java.io.tmpdir") + "/" + nodeIdentifier)

    if (!folder.exists) {
      folder.mkdirs()
    }

    if (!folder.exists || !folder.canRead || !folder.canWrite) {
      System.err.println(folder.getCanonicalPath + " does not exist or do not have permission to read and write. Exiting.")
      System.exit(-1)
    }

    new MapDBStore(new java.io.File(folder, "journal"), Int.MaxValue)
  }
}