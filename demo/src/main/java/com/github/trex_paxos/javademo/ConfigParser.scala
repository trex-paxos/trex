package com.github.trex_paxos.javademo

import com.github.trex_paxos.akka.{Cluster, Node}
import net.liftweb.json._
import net.liftweb.json.JsonDSL._

/**
 * JObject(
 * List(
 * JField(cluster,JObject(List(
 * JField(name,JString(testCluster)),
 * JField(folder,JString(testFolder)),
 * JField(retained,JInt(99)),
 * JField(nodes,JArray(List(
 * JObject(List(JField(nodeUniqueId,JInt(1)),Field(host,JString(host1)), JField(nodePort,JInt(111)), JField(clientPort,JInt(11)))),
 * JObject(List(JField(nodeUniqueId,JInt(2)), JField(host,JString(host2)), JField(nodePort,JInt(222)), JField(clientPort,JInt(22)))),
 * JObject(List(JField(nodeUniqueId,JInt(3)), JField(host,JString(host3)), JField(nodePort,JInt(333)), JField(clientPort,JInt(33))))))))))))
 * println(json)
 */
object ConfigParser {
  def parse(jsonString: String): Cluster = {
    val json = net.liftweb.json.parse(jsonString)

    val name = (json \ "cluster" \ "name") match {
      case JString(s) => s
      case x => throw new IllegalArgumentException("json \\ \"cluster\" \\ \"name\" should be a JString(s) but is " + x.toString)
    }

    val folder = (json \ "cluster" \ "folder") match {
      case JString(s) => s
      case x => throw new IllegalArgumentException("json \\ \"cluster\" \\ \"folder\" should be a JString(s) but is " + x.toString)
    }

    val retained = (json \ "cluster" \ "retained") match {
      case JInt(r) => r.toInt
      case x => throw new IllegalArgumentException("json \\ \"cluster\" \\ \"retained\" should be a JInt(i) but is " + x.toString)
    }

    // Node(nodeUniqueId = 1, host = "host1", clientPort = 11, nodePort =  111)
    def parseNode(json: JValue): Node = {
      val nodeUniqueId = (json \ "nodeUniqueId") match {
        case JInt(n) => n
        case x => throw new IllegalArgumentException("json \\ \"nodeUniqueId\" should be a JInt(n) but is " + x.toString)
      }
      val host = (json \ "host") match {
        case JString(h) => h
        case x => throw new IllegalArgumentException("json \\ \"host\" should be a JString(n) but is " + x.toString)
      }
      val clientPort = (json \ "clientPort") match {
        case JInt(n) => n
        case x => throw new IllegalArgumentException("json \\ \"clientPort\" should be a JInt(n) but is " + x.toString)
      }
      val nodePort = (json \ "nodePort") match {
        case JInt(n) => n
        case x => throw new IllegalArgumentException("json \\ \"nodePort\" should be a JInt(n) but is " + x.toString)
      }
      Node(nodeUniqueId.toInt, host, clientPort.toInt, nodePort.toInt)
    }

    val nodes = (json \ "cluster" \ "nodes") match {
      case JArray(a) => a map {
        n => parseNode(n)
      }
      case x => throw new IllegalArgumentException("json \\ \"cluster\" \\ \"nodes\" should be a JArray(a) but is " + x.toString)
    }

    Cluster(name, folder, retained, nodes)
  }

  def serialize(cluster: Cluster) = {
    val json = (
      "cluster" -> (
        ("name" -> cluster.name) ~
          ("folder" -> cluster.folder) ~
          ("retained" -> cluster.retained) ~
          ("nodes" -> cluster.nodes.map {
            n => ("nodeUniqueId" -> n.nodeUniqueId) ~
              ("host" -> n.host) ~
              ("nodePort" -> n.nodePort) ~
              ("clientPort" -> n.clientPort)
          })
        )
      )
    prettyRender(json)
  }
}
