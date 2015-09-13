package com.github.simbo1905.trex.internals

import com.github.simbo1905.trex.library.CommandValue

case class MembershipCommandValue(msgId: Long, members: Seq[ClusterMember]) extends CommandValue {
  override def bytes: Array[Byte] = emptyArray
}

case class ClusterMember(nodeUniqueId: Int, location: String, active: Boolean)

case class Membership(members: Seq[ClusterMember])