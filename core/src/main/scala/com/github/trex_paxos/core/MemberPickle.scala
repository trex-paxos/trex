package com.github.trex_paxos.core

import com.github.trex_paxos._

import scala.util.parsing.json._

object MemberPickle {

  // http://stackoverflow.com/a/4186090/329496
  class CC[T] {
    def unapply(a: Any): Option[T] = Some(a.asInstanceOf[T])
  }

  object M extends CC[Map[String, Any]]

  object L extends CC[List[Any]]

  object S extends CC[String]

  object D extends CC[Double]

  object B extends CC[Boolean]

  def s(a: Any) = "\"" + a + "\""

  def addressTo(address: Address): String = {
    "{" + s("host") + ":" + s(address.hostname) + ", " + s("port") + ":" + address.port + "}"
  }

  def addressesTo(addresses: Addresses) = {
    "{" + s("leader") + ":" + addressTo(addresses.leaderAddress) + ", " + s("peer") + ":" + addressTo(addresses.peerAddress) + "}"
  }

  def nodeTo(node: Node) = {
    //Node(nodeIdentifier: Int, addresses: Addresses)
    "{" + s("id") + ":" + node.nodeIdentifier + ", " + s("addrs") + ":" + addressesTo(node.addresses) + "}"
  }

  def nodeFrom(js: String): Option[Node] = {
    val json = JSON.parseFull(js)

    for {
      Some(M(map)) <- Option(json)
      D(id) = map("id")
      M(addrs) = map("addrs")

      M(leader) = addrs("leader")
      M(peer) = addrs("peer")

      S(lhost) = leader("host")
      D(lport) = leader("port")

      S(phost) = peer("host")
      D(pport) = peer("port")
    } yield {
      Node(id.toInt, Addresses(Address(lhost, lport.toInt), Address(phost, pport.toInt)))
    }
  }

  def quorumTo(quorum: Quorum) = {
    //Quorum(count: Int, of: Set[Weight])
    val weights = quorum.of map {
      case Weight(id, weight) => "{" + s("id") + ":" + id + ", " + s("w") + ":" + weight + "}"
    }
    "{" + s("count") + ":" + quorum.count + ", " + s("of") + ":" + weights.mkString("[", ",", "]") + "}"
  }

  def quorumFrom(js: String): Option[Quorum] = {
    val json = JSON.parseFull(js)
    for {
      Some(M(map: Map[String, Any])) <- Option(json)
      D(count) = map("count")
      L(weights) = map("of")
    } yield {
      val setOf: Set[Weight] = (
        for {
          M(weight) <- weights
          D(id) = weight("id")
          D(wt) = weight("w")
        } yield {
          Weight(id.toInt, wt.toInt)
        }
      ).toSet
      Quorum(count.toInt, setOf)
    }
  }

  def nodesFrom(js: String): Option[Set[Node]] = {
    val json = JSON.parseFull(js)
    Some((for {
      Some(M(map)) <- List(json)
      L(nodes: Seq[Any]) = map("nodes")

      node <- nodes
      M(n) = node

      D(id) = n("id")
      M(addrs) = n("addrs")

      M(leader) = addrs("leader")
      M(peer) = addrs("peer")

      S(lhost) = leader("host")
      D(lport) = leader("port")

      S(phost) = peer("host")
      D(pport) = peer("port")
    } yield {
      Node(id.toInt, Addresses(Address(lhost, lport.toInt), Address(phost, pport.toInt)))
    }).toSet)
  }

  def nodesTo(nodes: Set[Node]): String = {
    (nodes map {
      case node => nodeTo(node)
    }).mkString("[", ",", "]")
  }

  def toJson(e: Era): String = {
    val m = e.membership
    val slot = m.effectiveSlot match {
      case None => -1
      case Some(i) => i
    }
    "{" + s("era") + ":" + e.era + "," + s("slot") + ":" + slot + "," + s("qI") + ":" + quorumTo(m.quorumForPromises) + ", " + s("qII") + ":" + quorumTo(m.quorumForAccepts) + ", " + s("nodes") + ":" + nodesTo(m.nodes) + "}"
   }

  def fromJson(js: String): Option[Era] = {
    val json: Option[Any] = JSON.parseFull(js)
    for {
      Some(M(map)) <- Option(json)
      D(era) = map("era")
      D(slot) = map("slot")
      M(qI) = map("qI")
      D(countQI) = qI("count")
      L(ofQI) = qI("of")
      M(qII) = map("qII")
      D(countQII) = qII("count")
      L(ofQII) = qII("of")
      L(nodes: Seq[Any]) = map("nodes")
    } yield {

      val nds: Set[Node] = (for {
        node <- nodes
        M(n) = node

        D(id) = n("id")
        M(addrs) = n("addrs")

        M(leader) = addrs("leader")
        M(peer) = addrs("peer")

        S(lhost) = leader("host")
        D(lport) = leader("port")

        S(phost) = peer("host")
        D(pport) = peer("port")
      } yield {
        Node(id.toInt, Addresses(Address(lhost, lport.toInt), Address(phost, pport.toInt)))
      }).toSet

      val setOfQI: Set[Weight] = (
        for {
          M(weight) <- ofQI
          D(id) = weight("id")
          D(wt) = weight("w")
        } yield {
          Weight(id.toInt, wt.toInt)
        }
        ).toSet

      val setOfQII: Set[Weight] = (
        for {
          M(weight) <- ofQII
          D(id) = weight("id")
          D(wt) = weight("w")
        } yield {
          Weight(id.toInt, wt.toInt)
        }
        ).toSet

      val slotLong = slot.toLong
      val slotOption = slotLong match {
        case i if i >= 0 => Some(i)
        case _ => None
      }

      Era(era.toInt, Membership(Quorum(countQI.toInt, setOfQI), Quorum(countQII.toInt, setOfQII), nds, slotOption))
    }
  }

}
