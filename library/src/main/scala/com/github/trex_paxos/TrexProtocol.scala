package com.github.trex_paxos

/**
  * The UPaxos [[http://tessanddave.com/paxos-reconf-902f8b7.pdf]] voting weights. A weight of zero defined a learner.
  * See discussion at [[https://simbo1905.wordpress.com/2017/03/16/paxos-voting-weights/]]
  * @param nodeIdentifier The node identifier.
  * @param weight The voting weight.
  */
case class Weight(nodeIdentifier: Int, weight: Int)

/**
  * The FPaxos result means that simple majorities are not optimal. The UPaxos [[http://tessanddave.com/paxos-reconf-902f8b7.pdf]] also
  * highlights that we can safely change cluster configurations as long as we can specify overlapping quorums between
  * reconfigurations. We therefore want to be able to specify quorums such as  "half these nodes" in addition
  * to the traditional "a majority of these nodes". If we have four nodes we might specify a count of 3 for the PhaseI prepares
  * but 2 for the PhaseII accepts. If we double the voting weights of each node we would double the counts.
  *
  * @param count The number of vote required to achieve a quorum.
  * @param of    The nodes with their weights.
  */
case class Quorum(count: Int, of: Set[Weight])

/**
  * Represents an SocketAddress which the transport framework can resolve to an InetSocketAddress
  *
  * @param hostname The hostname
  * @param port     The socket port
  */
case class Address(hostname: String, port: Int)

/**
  * Represents the pair of socket addresses for a node server.
  *
  * @param leaderAddress The socket which clients connect to.
  * @param peerAddress   The socket peers in the Paxos cluster connect to.
  */
case class Addresses(leaderAddress: Address, peerAddress: Address)

/**
  * Represents a peer node in the cluster.
  *
  * @param nodeIdentifier The node unique ID which is to be used in Ballot numbers and which must be unique in time and space.
  * @param addresses      The pair of socket addresses for the node.
  */
case class Node(nodeIdentifier: Int, addresses: Addresses) {
  def peerAddressHostname = addresses.peerAddress.hostname
  def peerAddressPort = addresses.peerAddress.port
  def leaderAddressHostname = addresses.leaderAddress.hostname
  def leaderAddressPort = addresses.leaderAddress.port
}

/**
  * A cluster membership.
  *
  * @param effectiveSlot     The slot at which the membership comes into effect.
  * @param quorumForPromises The quorum for promises.
  * @param quorumForAccepts  The quorum for accepts.
  * @param nodes             The nodes in the cluster
  */
case class Membership(effectiveSlot: Long, quorumForPromises: Quorum, quorumForAccepts: Quorum, nodes: Set[Node]) {
  require(quorumForAccepts.of.intersect(quorumForPromises.of).nonEmpty,
    s"The quorum for promises must overlap with the quorum for accepts - quorumForPromises: ${quorumForPromises}, quorumForAccepts: ${quorumForAccepts}")

  def allQuorumNodes = quorumForPromises.of.map(_.nodeIdentifier) ++ quorumForAccepts.of.map(_.nodeIdentifier)

  def allLocated = nodes.map(_.nodeIdentifier)

  require(allQuorumNodes.intersect(allLocated) == allQuorumNodes,
    s"The unique nodes within the combined quorums don't match the nodes for which we have network addresses - quorumForPromises: ${quorumForPromises}, quorumForAccepts: ${quorumForAccepts}, nodes: ${nodes}, allQuorumNodes: ${allQuorumNodes}, allLocated: ${allLocated}")
}
