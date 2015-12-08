package com.github.trex_paxos.library

import org.scalatest.{Matchers, WordSpecLike}

import scala.collection.immutable.{SortedMap, TreeMap}
import scala.collection.mutable.ArrayBuffer

class CoreTests extends WordSpecLike with Matchers with PaxosLenses {
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

    val nodeData = PaxosData(progress, 0, 0, 3, TreeMap(), None, TreeMap(), Map.empty[Identifier, (CommandValue, String)])
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
        val newData = clientCommandsLens.set(nodeData, Map.empty[Identifier, (CommandValue, String)])
        assert(newData == nodeData)
      }
      {
        val commandValue = new CommandValue {
          override def msgId: Long = 0L

          override def bytes: Array[Byte] = Array()
        }
        val remoteRef = DummyRemoteRef()
        val clientCommands = Map(id ->(commandValue, remoteRef))
        val newData = clientCommandsLens.set(nodeData, clientCommands)
        assert(newData.clientCommands(id) == (commandValue -> remoteRef))
      }
    }

    "set leader state" in {
      {
        val newData = leaderLens.set(nodeData, (
          SortedMap.empty[Identifier, Map[Int, PrepareResponse]],
          SortedMap.empty[Identifier, AcceptResponsesAndTimeout],
          Map.empty[Identifier, (CommandValue, String)])
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
        val remoteRef = DummyRemoteRef()
        val clientCommands = Map(id ->(commandValue, remoteRef))
        val newData = leaderLens.set(nodeData, (prepareResponses, acceptResponses, clientCommands))
        assert(newData.prepareResponses(id) == Map.empty)
        assert(newData.acceptResponses(id) == AcceptResponsesAndTimeout(0L, a1, Map.empty))
        assert(newData.clientCommands(id) == (commandValue -> remoteRef))
      }
    }
  }

  val accept = Accept(Identifier(1, BallotNumber(1, 1), 98L), NoOperationCommandValue)

  "Backing down" should {
    "should reset data" in {
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
        clientCommands = Map(id ->(NoOperationCommandValue, DummyRemoteRef()))
      )
      val handler = new PaxosLenses with BackdownAgent
      val sentNoLongerLeader = Box(false)
      val io = new TestIO(new UndefinedJournal) {
        override def randomTimeout: Long = 99L

        override def sendNoLongerLeader(clientCommands: Map[Identifier, (CommandValue, String)]): Unit = sentNoLongerLeader(true)
      }
      // when we backdown
      val PaxosAgent(nuid, role, followerData) = handler.backdownAgent(io, PaxosAgent(0, Leader, leaderData))
      role shouldBe Follower
      nuid shouldBe 0
      // then it has a new timeout and the leader data is gone
      followerData.timeout shouldBe 99L
      followerData.prepareResponses.isEmpty shouldBe true
      followerData.epoch shouldBe None
      followerData.acceptResponses.isEmpty shouldBe true
      followerData.clientCommands.isEmpty shouldBe true
      sentNoLongerLeader() shouldBe true
    }
  }

  "Logging trait" should {

    class TestableLogger extends PaxosLogging {

      override def isErrorEnabled: Boolean = true

      override def isInfoEnabled: Boolean = true

      override def isDebugEnabled: Boolean = true

      override def isWarningEnabled: Boolean = true

      val captured = ArrayBuffer[String]()

      override def warning(msg: String): Unit = captured += "warning:" + msg

      override def warning(msg: String, one: Any): Unit = captured += "warning:" + msg + "," + one

      override def warning(msg: String, one: Any, two: Any): Unit = captured += "warning:" + msg + "," + one + "," + two

      override def warning(msg: String, one: Any, two: Any, three: Any): Unit = captured += "warning:" + msg + "," + one + "," + two + "," + three

      override def warning(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = captured += "warning:" + msg + "," + one + "," + two + "," + three + "," + four

      override def error(msg: String): Unit = captured += "error:" + msg

      override def error(msg: String, one: Any): Unit = captured += "error:" + msg + "," + one

      override def error(msg: String, one: Any, two: Any): Unit = captured += "error:" + msg + "," + one + "," + two

      override def error(msg: String, one: Any, two: Any, three: Any): Unit = captured += "error:" + msg + "," + one + "," + two + "," + three

      override def error(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = captured += "error:" + msg + "," + one + "," + two + "," + three + "," + four

      override def debug(msg: String): Unit = captured += "debug:" + msg

      override def debug(msg: String, one: Any): Unit = captured += "debug:" + msg + "," + one

      override def debug(msg: String, one: Any, two: Any): Unit = captured += "debug:" + msg + "," + one + "," + two

      override def debug(msg: String, one: Any, two: Any, three: Any): Unit = captured += "debug:" + msg + "," + one + "," + two + "," + three

      override def debug(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = captured += "debug:" + msg + "," + one + "," + two + "," + three + "," + four

      override def info(msg: String): Unit = captured += "info:" + msg

      override def info(msg: String, one: Any): Unit = captured += "info:" + msg + "," + one

      override def info(msg: String, one: Any, two: Any): Unit = captured += "info:" + msg + "," + one + "," + two

      override def info(msg: String, one: Any, two: Any, three: Any): Unit = captured += "info:" + msg + "," + one + "," + two + "," + three

      override def info(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = captured += "info:" + msg + "," + one + "," + two + "," + three + "," + four
    }

    "should log debug" in {
      val logger = new TestableLogger
      logger.isDebugEnabled shouldBe true
      logger.debug("hello")
      logger.captured(0) shouldBe "debug:hello"
      logger.captured.clear()
      logger.debug("hello", "world")
      logger.captured(0) shouldBe "debug:hello,world"
      logger.captured.clear()
      logger.debug("hello", "world", "again")
      logger.captured(0) shouldBe "debug:hello,world,again"
      logger.captured.clear()
      logger.debug("hello", "world", "again", "indeed")
      logger.captured(0) shouldBe "debug:hello,world,again,indeed"
      logger.captured.clear()
      logger.debug("hello", "world", "again", "indeed", "proceed")
      logger.captured(0) shouldBe "debug:hello,world,again,indeed,proceed"
    }
    "should log info" in {
      val logger = new TestableLogger
      logger.isInfoEnabled shouldBe true
      logger.info("hello")
      logger.captured(0) shouldBe "info:hello"
      logger.captured.clear()
      logger.info("hello", "world")
      logger.captured(0) shouldBe "info:hello,world"
      logger.captured.clear()
      logger.info("hello", "world", "again")
      logger.captured(0) shouldBe "info:hello,world,again"
      logger.captured.clear()
      logger.info("hello", "world", "again", "indeed")
      logger.captured(0) shouldBe "info:hello,world,again,indeed"
      logger.captured.clear()
      logger.info("hello", "world", "again", "indeed", "proceed")
      logger.captured(0) shouldBe "info:hello,world,again,indeed,proceed"
    }
    "should log warning" in {
      val logger = new TestableLogger
      logger.isWarningEnabled shouldBe true
      logger.warning("hello")
      logger.captured(0) shouldBe "warning:hello"
      logger.captured.clear()
      logger.warning("hello", "world")
      logger.captured(0) shouldBe "warning:hello,world"
      logger.captured.clear()
      logger.warning("hello", "world", "again")
      logger.captured(0) shouldBe "warning:hello,world,again"
      logger.captured.clear()
      logger.warning("hello", "world", "again", "indeed")
      logger.captured(0) shouldBe "warning:hello,world,again,indeed"
      logger.captured.clear()
      logger.warning("hello", "world", "again", "indeed", "proceed")
      logger.captured(0) shouldBe "warning:hello,world,again,indeed,proceed"
    }
    "should log error" in {
      val logger = new TestableLogger
      logger.isErrorEnabled shouldBe true
      logger.error("hello")
      logger.captured(0) shouldBe "error:hello"
      logger.captured.clear()
      logger.error("hello", "world")
      logger.captured(0) shouldBe "error:hello,world"
      logger.captured.clear()
      logger.error("hello", "world", "again")
      logger.captured(0) shouldBe "error:hello,world,again"
      logger.captured.clear()
      logger.error("hello", "world", "again", "indeed")
      logger.captured(0) shouldBe "error:hello,world,again,indeed"
      logger.captured.clear()
      logger.error("hello", "world", "again", "indeed", "proceed")
      logger.captured(0) shouldBe "error:hello,world,again,indeed,proceed"
    }
  }
}