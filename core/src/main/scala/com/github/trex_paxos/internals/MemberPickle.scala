package com.github.trex_paxos.internals

import scala.util.parsing.json._

object MemberPickle {

  def s(a: Any) = "\"" + a + "\""

  val statusFromString = Map(Learning.toString -> Learning, Accepting.toString -> Accepting)

  def toJson(membership: CommittedMembership): String = {
    val members: Seq[String] = membership.membership.members map {
      case Member(nodeUniqueId, location, clientLocation, memberStatus) =>
        "{" + s("n") + ":" + nodeUniqueId + ", " + s("l") + ":" + s(location) + "," + s("c") + ":" + s(clientLocation) + "," + s("s") + ":" + s(memberStatus.toString) + "}"
    }
    val ms = members.mkString("[", ",", "]")
    "{" + s("name") + ":" + s(membership.membership.name) + "," + s("slot") + ":" + membership.slot + "," + s("members") + ":" + ms + "}"
  }


  // http://stackoverflow.com/a/4186090/329496
  class CC[T] {
    def unapply(a: Any): Option[T] = Some(a.asInstanceOf[T])
  }

  object M extends CC[Map[String, Any]]

  object L extends CC[List[Any]]

  object S extends CC[String]

  object D extends CC[Double]

  object B extends CC[Boolean]

  def fromJson(jsonString: String): CommittedMembership = {
    val json = JSON.parseFull(jsonString)

    val Some((name, slot)) = for {
      Some(M(map)) <- Option(json)
      S(name) = map("name")
      D(slot) = map("slot")
    } yield {
      (name, slot.toLong)
    }

    val ms = for {
      Some(M(map)) <- List(json)
      L(members) = map("members")
      M(member) <- members
      D(nodeUniqueId) = member("n")
      S(location) = member("l")
      S(clientLocation) = member("c")
      S(status) = member("s")
    } yield {
      Member(nodeUniqueId.toInt, location, clientLocation, statusFromString(status))
    }

    CommittedMembership(slot, Membership(name, ms))
  }
}
