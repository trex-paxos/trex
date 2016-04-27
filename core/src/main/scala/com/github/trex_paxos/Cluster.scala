package com.github.trex_paxos

import com.typesafe.config.Config

/**
  * A node is an immutable addressable process within the cluster. If a process is moved to another location, such
  * that its address changes, it just leave the cluster and rejoin at another address, with a new node membershipId.
  *
  * @param membershipId The unique ID in the cluster for this addressable node. If the process moves host or port a new
  *                     membershipId must be assigned by having the node leave the cluster and rejoin it.
  * @param host The host of the node.
  * @param clientPort The client tcp port of the node.
  * @param nodePort The intercluster udp port of the node.
  */
case class Node(membershipId: Int, host: String, clientPort: Int, nodePort: Int)

case class Cluster(name: String, folder: String, retained: Int, nodes: Seq[Node]) {
  val nodeMap = nodes.map { n => (n.membershipId, n) }.toMap
}

object Cluster {

  def parseConfig(config: Config): Cluster = {
    val folder = config.getString("trex.data-folder")
    val name = config.getString("trex.cluster.name")
    val retained = config.getInt("trex.data-retained")
    val nodeIds: Array[String] = config.getString("trex.cluster.nodes").split(',')
    val nodes = nodeIds map { nodeId =>
      val host = config.getString(s"trex.cluster.node-$nodeId.host")
      val cport = config.getString(s"trex.cluster.node-$nodeId.client-port")
      val nport = config.getString(s"trex.cluster.node-$nodeId.node-port")
      Node(nodeId.toInt, host, cport.toInt, nport.toInt)
    }
    Cluster(name, folder, retained, collection.immutable.Seq(nodes: _*))
  }

}