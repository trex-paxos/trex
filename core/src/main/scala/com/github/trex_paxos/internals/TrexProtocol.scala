package com.github.trex_paxos.internals

import com.github.trex_paxos.library.CommandValue

case class MembershipCommandValue(msgId: Long, members: Seq[ClusterMember]) extends CommandValue {
  override def bytes: Array[Byte] = emptyArray
}

case class ClusterMember(nodeUniqueId: Int, location: String, active: Boolean)

case class Membership(members: Seq[ClusterMember])