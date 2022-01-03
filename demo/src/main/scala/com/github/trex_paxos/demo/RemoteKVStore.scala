package com.github.trex_paxos.demo

import akka.actor.TypedActor.MethodCall
import akka.actor._
import com.typesafe.config.ConfigFactory
//import org.mapdb.{DB, DBMaker}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

class MethodCallInvoker(target: Any) extends Actor with ActorLogging {
  override def receive: Receive = {
    case methodCall: MethodCall =>
      log.debug(s"$methodCall")
      val method = methodCall.method
      val parameters = methodCall.parameters
      Try {
        Option(method.invoke(target, parameters: _*))
      } match {
        case Failure(ex) =>
          log.error(ex, s"call to $method with $parameters got exception $ex")
          sender() ! ex
        case Success(response) => response.foreach {
          sender() ! _
        }
      }
  }
}

object RemoteKVStore {

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println("Usage: port folder")
      println("Where:\n\tport is the port to run the actor system")
      println("\tfolder is the folder to store data within")
    }
    val port = args(0).toInt
    val folderPath = args(1)
    println(s"starting actor system on port $port using folder:$folderPath")
    val folder = new java.io.File(folderPath)
    if (!folder.exists() || !folder.canRead || !folder.canWrite) {
      System.err.println(s"$folderPath does not exist or do not have permission to read and write. Exiting.")
      System.exit(-1)
    }
    val dataFolder = new java.io.File(folder.getCanonicalPath + "/" + port)

    //val db: DB = DBMaker.newFileDB(dataFolder).make
    val system =
      ActorSystem("ConsistentStore", ConfigFactory.load(s"remotestore-$port"))
    system.actorOf(Props(classOf[MethodCallInvoker], new MapDBConsistentKVStore(null)), "store")

    println(s"Started node $port with store $dataFolder")
  }
}

object KVStoreClient {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Args: host port")
      println("Where:\n\t host and port identify the remote actor system")
    }

    args.foreach(println(_))

    val host = args(0)
    val port = args(1).toInt

    val system =
      ActorSystem("LookupSystem", ConfigFactory.load("client"))

    val remotePath =
      s"akka.tcp://ConsistentStore@$host:$port/user/store"

    implicit val timeout = akka.util.Timeout(5 seconds)

    system.actorSelection(remotePath).resolveOne().onComplete {
      case Success(actorRefToRemoteActor) =>
        val typedActor: ConsistentKVStore =
          TypedActor(system).
            typedActorOf(
              TypedProps[ConsistentKVStore](),
              actorRefToRemoteActor)

        for (ln <- scala.io.Source.stdin.getLines()) {
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
      case Failure(ex) =>
        System.err.println(s"failed to resolve $remotePath in timeout $timeout")
    }

  }
}
