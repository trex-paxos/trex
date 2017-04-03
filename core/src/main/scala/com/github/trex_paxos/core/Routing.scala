package com.github.trex_paxos.core

import com.github.trex_paxos.library._

case class OutboundMessage(targets: Set[Int], msg: PaxosMessage)

object Routing {
  def route(nodeUniqueId: Int, paxosMessage: PaxosMessage, targets: Iterable[Int]): Iterable[OutboundMessage] = paxosMessage match {
    case m: RetransmitRequest => Seq(OutboundMessage(Set(m.to), m))
    case m: RetransmitResponse => Seq(OutboundMessage(Set(m.to), m))
    case m: AcceptResponse => Seq(OutboundMessage(Set(m.to), m))
    case m: PrepareResponse => Seq(OutboundMessage(Set(m.to), m))
    case m =>
       Seq(OutboundMessage(targets.toSet.filterNot(_ == nodeUniqueId), m))
  }
}
