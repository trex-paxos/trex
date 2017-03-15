package com.github.trex_paxos.core

import com.github.trex_paxos.library._

class EmptyLogging extends PaxosLogging {
  override def info(msg: String): Unit = {}

  override def info(msg: String, one: Any): Unit = {}

  override def info(msg: String, one: Any, two: Any): Unit = {}

  override def info(msg: String, one: Any, two: Any, three: Any): Unit = {}

  override def info(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = {}

  override def info(msg: String, one: Any, two: Any, three: Any, four: Any, five: Any): Unit = {}

  override def debug(msg: String): Unit = {}

  override def debug(msg: String, one: Any): Unit = {}

  override def debug(msg: String, one: Any, two: Any): Unit = {}

  override def debug(msg: String, one: Any, two: Any, three: Any): Unit = {}

  override def debug(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = {}

  override def error(msg: String): Unit = {}

  override def warning(msg: String): Unit = {}

  override def warning(msg: String, one: Any, two: Any): Unit = {}

  override def warning(msg: String, one: Any): Unit = {}

  override def warning(msg: String, one: Any, two: Any, three: Any): Unit = {}

  override def warning(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = {}

  override def error(msg: String, one: Any): Unit = {}

  override def error(msg: String, one: Any, two: Any): Unit = {}

  override def error(msg: String, one: Any, two: Any, three: Any): Unit = {}

  override def error(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = {}

  override def debug(msg: String, one: Any, two: Any, three: Any, four: Any, five: Any): Unit = {}

  override def error(msg: String, one: Any, two: Any, three: Any, four: Any, five: Any): Unit = {}

  override def warning(msg: String, one: Any, two: Any, three: Any, four: Any, five: Any): Unit = {}
}

object NoopPaxosLogging extends EmptyLogging

class UndefinedJournal extends Journal {
  override def saveProgress(progress: Progress): Unit = throw new AssertionError("deliberately not implemented")

  override def bounds: JournalBounds = throw new AssertionError("deliberately not implemented")

  override def loadProgress(): Progress = throw new AssertionError("deliberately not implemented")

  override def accepted(logIndex: Long): Option[Accept] = throw new AssertionError("deliberately not implemented")

  override def accept(a: Accept*): Unit = throw new AssertionError("deliberately not implemented")
}


case class DummyCommandValue(id: String) extends CommandValue {
  override def bytes: Array[Byte] = Array()

  override def msgUuid: String = id
}

object TestHelpers {

  import Ordering._
  import scala.collection.immutable.{TreeMap}

  val lowValue = Int.MinValue + 1

  val initialData = PaxosData(progress = Progress(
    highestPromised = BallotNumber(lowValue, lowValue),
    highestCommitted = Identifier(from = 0, number = BallotNumber(lowValue, lowValue), logIndex = 0)
  ), leaderHeartbeat = 0, timeout = 0, prepareResponses = TreeMap(), epoch = None, acceptResponses = TreeMap() )

  val initialQuorumStrategy = new DefaultQuorumStrategy(() => 3)

  val v1 = DummyCommandValue("1")
  val v2 = DummyCommandValue("2")
  val v3 = DummyCommandValue("3")

  val identifier11: Identifier = Identifier(1, BallotNumber(1, 1), 11L)
  val identifier12: Identifier = Identifier(2, BallotNumber(2, 2), 12L)
  val identifier13: Identifier = Identifier(2, BallotNumber(2, 2), 13L)
  val identifier14: Identifier = Identifier(2, BallotNumber(2, 2), 14L)

  val a11 = Accept(identifier11, v1)
  val a12 = Accept(identifier12, v2)
  val a13 = Accept(identifier13, v3)
  val a14 = Accept(identifier14, v3)

  def journaled11thru14(logIndex: Long): Option[Accept] = {
    logIndex match {
      case 11L => Option(a11)
      case 12L => Option(a12)
      case 13L => Option(a13)
      case 14L => Option(a14)
      case _ => None
    }
  }
}
