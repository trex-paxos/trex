package com.github.simbo1905.trex.library

import org.scalatest.{Matchers, WordSpecLike}

import scala.collection.immutable.{SortedMap, TreeMap}

class CoreSpec extends WordSpecLike with Matchers with PaxosLenses[TestClient] {
  "Paxos Numbers" should {
    "have working equalities" in {
      assert(BallotNumber(2, 2) > BallotNumber(1, 2))
      assert(BallotNumber(2, 2) > BallotNumber(2, 1))
      assert(!(BallotNumber(2, 2) > BallotNumber(2, 2)))

      assert(BallotNumber(2, 2) >= BallotNumber(1, 2))
      assert(BallotNumber(2, 2) >= BallotNumber(2, 1))
      assert(BallotNumber(2, 2) >= BallotNumber(2, 2))

      assert(BallotNumber(1, 1) < BallotNumber(2, 1))
      assert(BallotNumber(1, 1) < BallotNumber(1, 2))
      assert(!(BallotNumber(1, 2) < BallotNumber(1, 2)))

      assert(BallotNumber(1, 1) <= BallotNumber(2, 1))
      assert(BallotNumber(1, 1) <= BallotNumber(1, 2))
      assert(BallotNumber(1, 2) <= BallotNumber(1, 2))

      assert(!(BallotNumber(2, 1) <= BallotNumber(1, 1)))
    }
  }

  val progress = Progress(
    highestPromised = BallotNumber(0, 0),
    highestCommitted = Identifier(from = 0, number = BallotNumber(0, 0), logIndex = 0)
  )

  "Lens" should {

    import Ordering._

    val nodeData = PaxosData(progress, 0, 0, 3, TreeMap(), None, TreeMap(), Map.empty[Identifier, (CommandValue, TestClient)])
    val id = Identifier(0, BallotNumber(1, 2), 3L)

    "set prepare responses" in {
      {
        val newData = prepareResponsesLens.set(nodeData, SortedMap.empty[Identifier, Map[Int, PrepareResponse]])
        assert(newData == nodeData)
      }
      {
        val prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]] = TreeMap(id -> Map.empty)
        val newData = prepareResponsesLens.set(nodeData, prepareResponses)
        assert(newData.prepareResponses(id) == Map.empty)
      }
    }

    "set accept responses" in {
      {
        val newData = acceptResponsesLens.set(nodeData, SortedMap.empty[Identifier, AcceptResponsesAndTimeout])
        assert(newData == nodeData)
      }
      {
        val a1 = Accept(Identifier(1, BallotNumber(1, 1), 1L), ClientRequestCommandValue(0, Array[Byte](1, 1)))
        val acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout] = TreeMap(id -> AcceptResponsesAndTimeout(0L, a1, Map.empty))
        val newData = acceptResponsesLens.set(nodeData, acceptResponses)
        assert(newData.acceptResponses(id) == AcceptResponsesAndTimeout(0L, a1, Map.empty))
      }
    }

    "set client commands" in {
      {
        val newData = clientCommandsLens.set(nodeData, Map.empty[Identifier, (CommandValue, TestClient)])
        assert(newData == nodeData)
      }
      {
        val commandValue = new CommandValue {
          override def msgId: Long = 0L

          override def bytes: Array[Byte] = Array()
        }
        val clientRef = new TestClient
        val clientCommands = Map(id ->(commandValue, clientRef))
        val newData = clientCommandsLens.set(nodeData, clientCommands)
        assert(newData.clientCommands(id) == (commandValue -> clientRef))
      }
    }

    "set leader state" in {
      {
        val newData = leaderLens.set(nodeData, (
          SortedMap.empty[Identifier, Map[Int, PrepareResponse]],
          SortedMap.empty[Identifier, AcceptResponsesAndTimeout],
          Map.empty[Identifier, (CommandValue, TestClient)])
        )
        assert(newData == nodeData)
      }
      {
        val prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]] = TreeMap(id -> Map.empty)
        val a1 = Accept(Identifier(1, BallotNumber(1, 1), 1L), ClientRequestCommandValue(0, Array[Byte](1, 1)))
        val acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout] = TreeMap(id -> AcceptResponsesAndTimeout(0L, a1, Map.empty))
        val commandValue = new CommandValue {
          override def msgId: Long = 0L

          override def bytes: Array[Byte] = Array()
        }
        val clientRef = new TestClient
        val clientCommands = Map(id ->(commandValue, clientRef))
        val newData = leaderLens.set(nodeData, (prepareResponses, acceptResponses, clientCommands))
        assert(newData.prepareResponses(id) == Map.empty)
        assert(newData.acceptResponses(id) == AcceptResponsesAndTimeout(0L, a1, Map.empty))
        assert(newData.clientCommands(id) == (commandValue -> clientRef))
      }
    }
  }

  val accept = Accept(Identifier(1, BallotNumber(1, 1), 98L), NoOperationCommandValue)

  "Backing down" should {
    "should reset Paxos data" in {
      import Ordering._
      // given lots of leadership data
      val number = BallotNumber(Int.MinValue, Int.MinValue)
      val id = Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), 0)
      val leaderData = PaxosData(
        progress = Progress(
          number, id
        ),
        leaderHeartbeat = 0,
        timeout = 0,
        clusterSize = 3,
        prepareResponses = TreeMap(id -> Map.empty),
        epoch = Some(number),
        acceptResponses = TreeMap(id -> AcceptResponsesAndTimeout(0, accept, Map.empty)),
        clientCommands = Map(id ->(NoOperationCommandValue, new TestClient))
      )
      val handler = new PaxosLenses[TestClient] with BackdownData[TestClient]
      var sentNoLongerLeader = false
      val io = new TestIO(new UndefinedJournal) {
        override def randomTimeout: Long = 99L

        override def sendNoLongerLeader(clientCommands: Map[Identifier, (CommandValue, TestClient)]): Unit = sentNoLongerLeader = true
      }
      // when we backdown
      val followerData = handler.backdownData(io, leaderData)
      // then it has a new timeout and the leader data is gone
      followerData.timeout shouldBe 99L
      followerData.prepareResponses.isEmpty shouldBe true
      followerData.epoch shouldBe None
      followerData.acceptResponses.isEmpty shouldBe true
      followerData.clientCommands.isEmpty shouldBe true
      sentNoLongerLeader shouldBe true
    }
  }
}