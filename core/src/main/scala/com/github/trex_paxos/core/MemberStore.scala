package com.github.trex_paxos.core

import com.github.trex_paxos.Membership

trait MemberStore {
  def saveMembership(slot: Long, membership: Membership): Unit
  def loadMembership(): Option[Membership]
}
