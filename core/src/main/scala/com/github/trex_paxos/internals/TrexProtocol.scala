package com.github.trex_paxos.internals

case class Membership(effectiveSlot: Long, quorumForPromises: Set[Int], quorumForAccepts: Set[Int], locations: Map[Int,String])
