package com.github.trex_paxos.internals

import com.github.trex_paxos.internals.MemberStatus.MemberStatus

object MemberStatus extends Enumeration {
  type MemberStatus = Value
  val Learning = Value("Learning")
  val Accepting = Value("Accepting")
}

//object MemberStatus {
//  def resolve(id: Int) = id match {
//    case 0 => Learning
//    case 1 => Accepting
//  }
//  def apply(id: Int) = resolve(id)
//  def unapply(ms: MemberStatus): Int = ms.id
//}
//
//sealed trait MemberStatus {
//  def id: Int
//}
//
///**
//  * The node is not a part of any quorums so does not respond to prepare or accept messages from a leader.
//  * It isn't eligible to become a leader.
//  */
//case object Learning extends MemberStatus {
//  val id: Int = 0
//}
//
///**
//  * The node is part of any quorums and so responds to prepare or accept messages from a leader.
//  * The node is eligible to become a leader.
//  */
//case object Accepting extends MemberStatus {
//  val id: Int = 1
//}

object Member {
  val pattern = "(.+):([0-9]+)".r
}

/**
  * Details of a member of the current paxos cluster. Note that the actual transports and discovery are abstract to
  * this class it uses strings so that concrete implementations can use different protocols.
  * @param nodeUniqueId The unique paxos number for this membership
  * @param location     The location for server-to-server typically given as "host:port" but could be a url
  * @param clientLocation The location for client-to-server typically given as "host:port" but could be a url
  * @param status       The status of the member.
  */
private[trex_paxos] case class Member(nodeUniqueId: Int, location: String, clientLocation: String, status: MemberStatus)

/**
  * A complete Paxos cluster.
 *
  * @param name The cluster name.
  * @param members The unique members of the cluster.
  */
private[trex_paxos] case class Membership(name: String, members: Seq[Member])


/**
  * A membership becomes committed at a slot index and a client can state the slot it last knew the membership. A
  * server in the cluster can then know if it is stale and reply with the latest membership.
  * @param slot The log index at which the membership was committed.
  * @param membership The cluster membership.
  */
case class CommittedMembership(slot: Long,  membership: Membership)
