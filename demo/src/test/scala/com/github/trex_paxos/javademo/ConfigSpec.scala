package com.github.trex_paxos.javademo

import com.github.trex_paxos.akka.{Cluster, Node}
import org.scalatest._
import org.scalatest.matchers.should._

class ConfigSpec extends wordspec.AnyWordSpec with Matchers with BeforeAndAfter  {

  val cluster = Cluster(name="testCluster",folder="testFolder",retained = 99, nodes = Seq(
    Node(nodeUniqueId = 1, host = "host1", clientPort = 11, nodePort =  111),
    Node(nodeUniqueId = 2, host = "host2", clientPort = 22, nodePort =  222),
    Node(nodeUniqueId = 3, host = "host3", clientPort = 33, nodePort =  333),
  ) )

  "Config" should {
    var rendered: String = null
    "be written out from case classes" in {
      rendered = ConfigParser.serialize(cluster)
      println(rendered)
      rendered shouldBe "{\n  \"cluster\":{\n    \"name\":\"testCluster\",\n    \"folder\":\"testFolder\",\n    \"retained\":99,\n    \"nodes\":[\n      {\n        \"nodeUniqueId\":1,\n        \"host\":\"host1\",\n        \"nodePort\":111,\n        \"clientPort\":11\n      },\n      {\n        \"nodeUniqueId\":2,\n        \"host\":\"host2\",\n        \"nodePort\":222,\n        \"clientPort\":22\n      },\n      {\n        \"nodeUniqueId\":3,\n        \"host\":\"host3\",\n        \"nodePort\":333,\n        \"clientPort\":33\n      }\n    ]\n  }\n}"
    }
    "be loaded back from case classes" in {
      val cluster2 = ConfigParser.parse(rendered)
      println(cluster2)
      cluster2.equals(cluster) shouldBe true
    }
  }
}
