package com.github.trex_paxos.netty

import java.util.Base64

import com.github.trex_paxos.ClusterConfiguration
import com.github.trex_paxos.core.{MemberPickle, ClusterConfigurationStore}
import com.github.trex_paxos.library._
import org.slf4j.LoggerFactory

import scala.collection.immutable.Seq
import scala.util.{Failure, Success, Try}

object ClusterDriver {
  def route(msg: AnyRef): Option[Int] = msg match {
    case acceptResponse: AcceptResponse =>
      Option(acceptResponse.to)
    case prepareResponse: PrepareResponse =>
      Option(prepareResponse.to)
    case retransmitResponse: RetransmitResponse =>
      Option(retransmitResponse.to)
    case retransmitRequest: RetransmitRequest =>
      Option(retransmitRequest.to)
    case x =>
      None // broadcast
  }
}

class ClusterDriver(val nodeIdentifier: Int, val memberStore: ClusterConfigurationStore, deserialize: (Array[Byte]) => Try[Any]) {
  val logger = LoggerFactory.getLogger(this.getClass)

  import ClusterDriver._

  protected var peers = peersFor(memberStore.loadForHighestEra().getOrElse(throw new IllegalArgumentException("Uninitiated ClusterConfigurationStore")))

  def peersFor(e: ClusterConfiguration): Map[Int, Client] = {
    (e.nodes flatMap {
      case n if n.nodeIdentifier != nodeIdentifier =>
        Some(n.nodeIdentifier -> new Client(n))
      case _ =>
        None
    }).toMap
  }

  def transmitMessages(msgs: Seq[PaxosMessage]): Unit = msgs foreach {
    case outbound =>
      val clients =
        route(outbound) match {
          case Some(nodeId) =>
            Seq(peers.get(nodeId).getOrElse(throw new IllegalArgumentException(s"$nodeId is not in $peers")))
          case _ => peers.values
        }
      logger.debug("clients is {} from peers {} for msg {}", clients, peers, outbound)
      clients.foreach(_.send(outbound))
  }

  val deliverMembership: PartialFunction[Payload, Array[Byte]] = {
    case p@Payload(Identifier(_, _, logIndex), ClusterCommandValue(msgUuid, bytes)) =>
      logger.debug("received ClusterCommandValue {}", p)
      deserialize(bytes) match {
        case Success(m: ClusterConfiguration) =>
          logger.info("received for slot {} with m {}", logIndex: Any, m: Any)
          memberStore.loadForHighestEra() match {
            case None =>
              val msg = "uninitialised member store"
              logger.error(msg)
              throw new IllegalArgumentException()
            case Some(_) =>
              val nextEra = memberStore.saveAndAssignEra(m)
              peers = peersFor(nextEra)
              MemberPickle.toJson(nextEra).getBytes("UTF8")
          }
        case Success(x) =>
          logger.error("unable to deserialize bytes to get a ClusterConfiguration got a {} from {}", x: Any, Base64.getEncoder.encodeToString(bytes): Any)
          throw new IllegalArgumentException(s"not a ClusterConfiguration: ${x}")
        case Failure(ex) =>
          throw ex
      }
  }


}
