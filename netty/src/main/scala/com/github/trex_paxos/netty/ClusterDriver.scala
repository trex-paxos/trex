package com.github.trex_paxos.netty

import java.util.Base64

import com.github.trex_paxos.Membership
import com.github.trex_paxos.core.MemberStore
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

class ClusterDriver(val memberStore: MemberStore, deserialize: (Array[Byte]) => Try[Any]) {
  val logger = LoggerFactory.getLogger(this.getClass)

  import ClusterDriver._

  protected var peers = peersFor(memberStore.loadMembership().getOrElse(throw new IllegalArgumentException("Uninitiated MemberStore")))

  def peersFor(m: Membership): Map[Int, Client] = {
    (m.nodes map {
      case n =>
        n.nodeIdentifier -> new Client(n)
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

  val deliverMembership: PartialFunction[Payload, Any] = {
    case p@Payload(logIndex, ClusterCommandValue(msgUuid, bytes)) =>
      logger.debug("received ClusterCommandValue {}", p)
      deserialize(bytes) match {
        case Success(m: Membership) =>
          logger.info("received for slot {} with m {}", logIndex: Any, m: Any)
          memberStore.saveMembership(logIndex, m)
          peers = peersFor(m)
        case Success(x) =>
          logger.error("unable to deserialize bytes to get a Membership got a {} from {}", x: Any, Base64.getEncoder.encodeToString(bytes): Any)
          throw new IllegalArgumentException(s"not a Membership: ${x}")
        case Failure(ex) =>
          throw ex
      }
  }


}
