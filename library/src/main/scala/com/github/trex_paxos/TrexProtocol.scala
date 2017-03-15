package com.github.trex_paxos

/**
  * The FPaxos result means that simple majorities are not optimal. The UPaxos [[http://tessanddave.com/paxos-reconf-902f8b7.pdf]] also
  * highlights that we can safely change cluster configurations as long as we can specify overlapping quorums between
  * reconfigurations. We therefore want to be able to specify quorums such as  "all of these specific nodes" in addition
  * to the traditional "any majority of these nodes".
  * TODO should have weights.
  * @param count The number of nodes in the specified set required to achieve a quorum.
  * @param of    The nodes from which the quorum may be formed.
  */
case class Quorum(count: Int, of: Set[Int])

/**
  * A cluster membership.
  *
  * @param effectiveSlot     The slot at which the membership comes into effect.
  * @param quorumForPromises The quorum for promises.
  * @param quorumForAccepts  The quorum for accepts.
  * @param locations         The network locations of the members of the set
  */
case class Membership(effectiveSlot: Long, quorumForPromises: Quorum, quorumForAccepts: Quorum, locations: Map[Int, String]) {
  require(quorumForAccepts.of.intersect(quorumForPromises.of).nonEmpty,
    s"The quorum for promises must overlap with the quorum for accepts - quorumForPromises: ${quorumForPromises}, quorumForAccepts: ${quorumForAccepts}")
  def allQuorumNodes = quorumForPromises.of ++ quorumForAccepts.of
  def allLocated = locations.keys.toSet
  require(allQuorumNodes.intersect(allLocated) == allQuorumNodes,
    s"Network locations are required for all nodes - quorumForPromises: ${quorumForPromises}, quorumForAccepts: ${quorumForAccepts}, locations: ${locations}")
}
