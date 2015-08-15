package com.github.simbo1905.trexdemo

import java.net.InetSocketAddress

import akka.actor._
import scala.language.postfixOps
import scala.concurrent.duration._
import akka.util.Timeout
import com.github.simbo1905.trex._
import com.github.simbo1905.trex.internals._
import com.typesafe.config._
import org.mapdb.{DB, DBMaker}

object TrexKVClient {
  def usage(): Unit = {
    println("Args: conf")
    println("Where:\tconf is the config file defining the cluster")
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      usage()
      System.exit(1)
    }
    args.foreach(println(_))
    val config = ConfigFactory.load(args(0))
    val cluster = Cluster.parseConfig(config)

    val system =
      ActorSystem(cluster.name, ConfigFactory.load("client.conf"))

    val timeout = Timeout(100 millisecond)

    val driver = system.actorOf(Props(classOf[StaticClusterDriver], timeout, cluster, 20), "TrexDriver")

    val typedActor: ConsistentKVStore =
      TypedActor(system).
        typedActorOf(
          TypedProps[ConsistentKVStore],
          driver)

    println("commands:\n\n\tget key\n\tput key value\n\tput key value version\n\tremove key\n\tremove key version\n\nlooping on stdin for your commands...")

    for (ln <- io.Source.stdin.getLines) {
      val args = ln.split("\\s+")

      args(0) match {
        case "put" =>
          (args.length match {
            case 3 =>
              typedActor.put(args(1), args(2))
            case 4 =>
              typedActor.put(args(1), args(2), args(3).toLong)
          }) match {
            case true => println("true")
            case false => println("false")
            case _: Unit => println("Unit") // unversioned returns unit
          }
        case "get" =>
          val response = typedActor.get(args(1))
          println(s"${args(1)}->${response}")
        case "remove" =>
          (args.length match {
            case 2 =>
              typedActor.remove(args(1))
            case 3 =>
              typedActor.remove(args(1), args(2).toLong)
          }) match {
            case true => println("true")
            case false => println("false")
            case _: Unit => println("Unit") // unversioned returns unit
          }
        case "quit" | "exit" | "bye" =>
          println("goodbye")
          System.exit(0)
        case x => println(s"unknown command $x")
      }
    }
  }
}

object TrexKVStore {
  def usage(): Unit = {
    println("Args: conf nodeId")
    println("Where:\tconf is the config file defining the cluster")
    println("\t\tnodeId is node identifier to start")
  }

  def main(args: Array[String]): Unit = {
    import System.err
    if (args.length != 2) {
      usage()
      System.exit(1)
    }
    args.foreach(println(_))
    val config = ConfigFactory.load(args(0))
    val cluster = Cluster.parseConfig(config)
    val nodeId = args(1).toInt

    println(cluster)
    val nodeMap = cluster.nodes.map(node => (node.id, node)).toMap

    if (nodeMap.get(nodeId).isDefined) {

      val folder = new java.io.File(cluster.folder + "/" + nodeId)
      if (!folder.exists() || !folder.canRead || !folder.canWrite) {
        err.println(s"${folder.getCanonicalPath} does not exist or do not have permission to read and write. Exiting.")
        System.exit(-1)
      }
      // the client app K-V store
      val dataFile = new java.io.File(folder.getCanonicalPath + "/kvstore")
      println(s"node kv data store is ${dataFile.getCanonicalPath}")
      val db: DB = DBMaker.newFileDB(dataFile).make
      val clientApp = new MapDBConsistentKVStore(db)
      val logFile = new java.io.File(folder.getCanonicalPath + "/paxos")
      println(s"paxos data log is ${logFile.getCanonicalPath}")
      val journal = new FileJournal(logFile, cluster.retained)
      // the node unique id in the paxos closter which is passed into main
      val node = nodeMap.get(nodeId).get
      // actor system with the node config
      val system =
        ActorSystem(cluster.name, ConfigFactory.load("server.conf").withValue("akka.remote.netty.tcp.port",ConfigValueFactory.fromAnyRef(node.clientPort) ))
      // generic entry point accepts TypedActor MethodCall messages and reflectively invokes them on our client app
      system.actorOf(Props(classOf[TypedActorPaxosEndpoint], cluster, PaxosActor.Configuration(config, cluster.nodes.size), node.id, journal, clientApp, "TrexServer"))

    } else {
      err.println(s"$nodeId is not a valid node number in cluster $cluster")
    }
  }
}

// TODO change store to AnyRef and move into core
class TypedActorPaxosEndpoint(cluster: Cluster, config: PaxosActor.Configuration, nodeUniqueId: Int, journal: Journal, client: AnyRef) extends Actor with ActorLogging {
  val selfNode = cluster.nodeMap(nodeUniqueId)

  val peerNodes = cluster.nodes.filterNot(_.id==nodeUniqueId)

  val peers: Map[Int,ActorRef] = Cluster.senders(context.system, peerNodes)

  val broadcast = context.system.actorOf(Props(classOf[Broadcast], peers.values.toSeq), "broadcast")

  val kvStore = context.system.actorOf(Props(classOf[TypedActorPaxosEndpoint], config, broadcast, nodeUniqueId, journal, client), "PaxosActor")

  val listener = context.system.actorOf(Props(classOf[UdpListener], new InetSocketAddress(selfNode.host, selfNode.nodePort), self), "UdpListener")

  override def receive: Receive = {
    case outbound: AnyRef if sender == kvStore =>
      route(outbound) ! outbound
    case inbound: AnyRef if sender == listener =>
      kvStore ! inbound
    case t: Terminated =>
      // TODO handle this
      log.warning(s"Termination notice $t")
    case unknown =>
      log.warning("{} unknown message {} from {}", this.getClass.getCanonicalName, unknown, sender)
  }

  def route(msg: AnyRef): ActorRef = {
    val nodeId = msg match {
      case acceptResponse: AcceptResponse =>
        acceptResponse.requestId.from
      case prepareResponse: PrepareResponse =>
        prepareResponse.requestId.from
      case retransmitResponse: RetransmitResponse =>
        retransmitResponse.to
      case retransmitRequest: RetransmitRequest =>
        retransmitRequest.to
    }
    log.debug("routing to {} message {}", nodeId, msg)
    peers(nodeId)
  }
}
