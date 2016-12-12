package com.github.trex_paxos.demo

import akka.actor._
import akka.util.Timeout
import com.github.trex_paxos.internals._
import com.github.trex_paxos._
import com.typesafe.config._
import org.mapdb.{DB, DBMaker}

import scala.concurrent.duration._
import scala.language.postfixOps

object TrexKVClient {
  def usage(): Unit = {
    println("Args: conf [hostname]")
    println("Where:\tconf is the config file defining the cluster")
    println("Where:\thostname is the optional address of inbound response defaults to 127.0.0.1")
  }

  def main(args: Array[String]): Unit = {
    val argMap: Map[Int, String] = (args zipWithIndex).toMap.map(_.swap)

    if (args.length != 1 && args.length != 2) {
      usage()
      System.exit(1)
    }

    args.foreach(println(_))

    val configName = args(0)
    val config = ConfigFactory.load(configName)
    val cluster = Cluster.parseConfig(config)
    val hostname = argMap.getOrElse(1, "127.0.0.1")

    val systemConfig = ConfigFactory.load(configName).withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(hostname))

    println(systemConfig)

    val system =
      ActorSystem(cluster.name, systemConfig)

    val timeout = Timeout(100 millisecond)

    val driver = system.actorOf(Props(classOf[DynamicClusterDriver], timeout, 20), "TrexDriver")

    val initialize = DynamicClusterDriver(cluster)

    driver ! initialize

    val typedActor: ConsistentKVStore =
      TypedActor(system).
        typedActorOf(
          TypedProps[ConsistentKVStore],
          driver)

    println("commands:\n\n\tget key\n\tput key value\n\tput key value version\n\tremove key\n\tremove key version\n\nlooping on stdin for your commands...")

    for (ln <- io.Source.stdin.getLines()) {
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
          println(s"${args(1)}->$response")
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
    println("\t\texample: server3.conf [2552|2562|2572]")
  }

  def main(args: Array[String]): Unit = {
    import System.err
    if (args.length != 2) {
      usage()
      System.exit(1)
    }
    args.foreach(println(_))
    val configName = args(0)
    val nodeId = args(1).toInt
    val config = ConfigFactory.load(configName)
    val cluster = Cluster.parseConfig(config)
    val node = cluster.nodeMap.getOrElse(nodeId, throw new IllegalArgumentException(s"No node $nodeId in $cluster"))

    println(cluster)
    val nodeMap = cluster.nodes.map(node => (node.nodeUniqueId, node)).toMap

    nodeMap.get(nodeId) match {
      case Some(node) =>
        val folder = new java.io.File(cluster.folder + "/" + nodeId)
        if( !folder.exists() ) folder.mkdirs()
        if (!folder.exists() || !folder.canRead || !folder.canWrite) {
          err.println(s"${folder.getCanonicalPath} does not exist or do not have permission to read and write. Exiting.")
          System.exit(-1)
        }
        // the client app K-V store
        val dataFile = new java.io.File(folder.getCanonicalPath + "/kvstore")
        println(s"node kv data store is ${dataFile.getCanonicalPath}")
        val db: DB = DBMaker.newFileDB(dataFile).make
        val target = new MapDBConsistentKVStore(db)
        val logFile = new java.io.File(folder.getCanonicalPath + "/paxos")
        println(s"paxos data log is ${logFile.getCanonicalPath}")
        val journal = new MapDBStore(logFile, cluster.retained)
        val systemConfig = ConfigFactory.load(configName)
          .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(node.clientPort))
          .withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(node.host))
        println(systemConfig.toString)
        // actor system with the node config
        val system = ActorSystem(cluster.name, systemConfig)

        val conf: PaxosProperties = PaxosProperties(config)

        TrexServer.initializeIfEmpty(cluster, journal)

        system.actorOf(TrexServer(conf, classOf[TypedActorPaxosEndpoint], node, journal, journal, target), "PaxosActor")

      case None => err.println(s"$nodeId is not a valid node number in cluster $cluster")
    }
  }
}

