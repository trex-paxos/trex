package com.github.trex_paxos

import com.typesafe.config.Config

case class Cluster(name: String, folder: String, retained: Int, nodes: Seq[Node]) {
  val nodeMap = nodes.map { n => (n.nodeIdentifier, n) }.toMap
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
      val addresses = Addresses(Address(host, cport.toInt), Address(host, nport.toInt))
      Node(nodeId.toInt, addresses)
    }
    Cluster(name, folder, retained, collection.immutable.Seq(nodes: _*))
  }

}