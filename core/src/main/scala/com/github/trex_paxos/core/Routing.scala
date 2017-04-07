package com.github.trex_paxos.core

import com.github.trex_paxos.library._

sealed trait OutboundMessage

case class OutboundClusterMessage(targets: Set[Int], msg: PaxosMessage) extends OutboundMessage

/**
  * @param logIndex    The paxos slot at which the command was chosen.
  * @param clientMsgId The id of the ClientCommandValue being responded to.
  * @param response    The result of running the comand value.
  */
case class ServerResponse(logIndex: Long, clientMsgId: String, val response: Option[Array[Byte]])  extends OutboundMessage

object Routing {
  /**
    * Routes a message to the appropriate node or nodes based on the message type.
    *
    * @param nodeUniqueId The current nodes unique id
    * @param paxosMessage The message to route
    * @param targets The nodes eligible to receive a broadcast message type which may be reduced during a UPaxos leader overlap
    */
  def route(nodeUniqueId: Int, paxosMessage: PaxosMessage, targets: Iterable[Int]): Iterable[OutboundClusterMessage] = paxosMessage match {
    case m: ResponseMessage  => Seq(OutboundClusterMessage(Set(m.to), m))
    case m =>
       Seq(OutboundClusterMessage(targets.toSet.filterNot(_ == nodeUniqueId), m))
  }
}
