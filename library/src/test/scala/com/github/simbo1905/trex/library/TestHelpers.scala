package com.github.simbo1905.trex.library

import com.github.simbo1905.trex.library.Ordering._

import scala.collection.immutable.{SortedMap, TreeMap}

case class DummyRemoteRef(val number: Int = 0)

class UndefinedIO extends PaxosIO[DummyRemoteRef] {
  override def journal: Journal = throw new AssertionError("deliberately not implemented")

  override def plog: PaxosLogging = throw new AssertionError("deliberately not implemented")

  override def minPrepare: Prepare = throw new AssertionError("deliberately not implemented")

  override def randomTimeout: Long = throw new AssertionError("deliberately not implemented")

  override def clock: Long = throw new AssertionError("deliberately not implemented")

  override def send(msg: PaxosMessage): Unit = throw new AssertionError("deliberately not implemented")

  override def deliver(value: CommandValue): Any = throw new AssertionError("deliberately not implemented")

  override def sendNoLongerLeader(clientCommands: Map[Identifier, (CommandValue, DummyRemoteRef)]): Unit = throw new AssertionError("deliberately not implemented")

  override def respond(client: DummyRemoteRef, data: Any): Unit = throw new AssertionError("deliberately not implemented")

  override def sender: DummyRemoteRef = throw new AssertionError("deliberately not implemented")
}

trait SilentLogging {
  this: UndefinedIO =>
  override def plog: PaxosLogging = NoopPaxosLogging
}

class UndefinedPrepareResponse extends PrepareResponse {
  override def requestId: Identifier = throw new AssertionError("deliberately not implemented")

  override def highestAcceptedIndex: Long = throw new AssertionError("deliberately not implemented")

  override def leaderHeartbeat: Long = throw new AssertionError("deliberately not implemented")

  override def progress: Progress = throw new AssertionError("deliberately not implemented")

  override def from: Int = throw new AssertionError("deliberately not implemented")
}

class UndefinedJournal extends Journal {
  override def save(progress: Progress): Unit = throw new AssertionError("deliberately not implemented")

  override def bounds: JournalBounds = throw new AssertionError("deliberately not implemented")

  override def load(): Progress = throw new AssertionError("deliberately not implemented")

  override def accepted(logIndex: Long): Option[Accept] = throw new AssertionError("deliberately not implemented")

  override def accept(a: Accept*): Unit = throw new AssertionError("deliberately not implemented")
}

case class MessageAndTimestamp(msg: PaxosMessage, ts: Long)

class TestIO(j: Journal) extends UndefinedIO {
  var sent: Seq[MessageAndTimestamp] = Seq()

  override def send(msg: PaxosMessage): Unit = {
    sent = sent :+ MessageAndTimestamp(msg, System.nanoTime)
  }

  override def journal: Journal = j

  override def randomTimeout: Long = 12345L

  override def plog: PaxosLogging = NoopPaxosLogging
}

class UndefinedAcceptResponse extends AcceptResponse {
  override def requestId: Identifier = throw new AssertionError("deliberately not implemented")

  override def from: Int = throw new AssertionError("deliberately not implemented")

  override def progress: Progress = throw new AssertionError("deliberately not implemented")
}

case class TimeAndParameter(time: Long, parameter: Any)

object TestHelpers extends PaxosLenses[DummyRemoteRef] {
  val undefinedIO = new UndefinedIO

  val undefinedSilentIO = new UndefinedIO with SilentLogging

  val undefinedIOwithNoopLogging = new UndefinedIO {
    override def plog: PaxosLogging = NoopPaxosLogging
  }

  val noopJournal = new Journal {
    override def save(progress: Progress): Unit = {}

    override def bounds: JournalBounds = JournalBounds(0L, 0L)

    override def load(): Progress = TestHelpers.initialData.progress

    override def accepted(logIndex: Long): Option[Accept] = None

    override def accept(a: Accept*): Unit = {}
  }

  val negativeClockIO = new UndefinedIO {
    override def clock: Long = Long.MinValue
  }

  val maxClockIO = new UndefinedIO {
    override def clock: Long = Long.MaxValue
  }

  val paxosAlgorithm = new PaxosAlgorithm[DummyRemoteRef]

  val lowValue = Int.MinValue + 1

  val initialData = PaxosData[DummyRemoteRef](
    progress = Progress(
      highestPromised = BallotNumber(lowValue, lowValue),
      highestCommitted = Identifier(from = 0, number = BallotNumber(lowValue, lowValue), logIndex = 0)
    ),
    leaderHeartbeat = 0,
    timeout = 0,
    clusterSize = 3, prepareResponses = TreeMap(), epoch = None, acceptResponses = TreeMap(), clientCommands = Map.empty[Identifier, (CommandValue, DummyRemoteRef)])

  val undefinedPrepareResponse = new UndefinedPrepareResponse

  val undefinedAcceptResponse = new UndefinedAcceptResponse

  val identifier98: Identifier = Identifier(1, BallotNumber(2, 2), 98L)

  val a98 = Accept(identifier98, NoOperationCommandValue)

  val emptyAcceptResponses98: SortedMap[Identifier, AcceptResponsesAndTimeout] = TreeMap(
    a98.id -> AcceptResponsesAndTimeout(100L, a98, Map.empty)
  )

  val prepare = Prepare(Identifier(0, BallotNumber(lowValue + 1, 0), 1L))

  val prepareSelfVotes = SortedMap.empty[Identifier, Map[Int, PrepareResponse]] ++
    Seq((prepare.id -> Map(0 -> PrepareAck(prepare.id, 0, initialData.progress, 0, 0, None))))

  val v3 = ClientRequestCommandValue(2, Array[Byte](2))
  val identifier99: Identifier = Identifier(2, BallotNumber(2, 2), 99L)
  val identifier100: Identifier = Identifier(3, BallotNumber(3, 3), 100L)
  val a99 = Accept(identifier99, NoOperationCommandValue)

  val a100 = Accept(identifier100, v3)

  val emptyAcceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout] = TreeMap(
    a98.id -> AcceptResponsesAndTimeout(100L, a98, Map.empty),
    a99.id -> AcceptResponsesAndTimeout(50L, a99, Map.empty),
    a100.id -> AcceptResponsesAndTimeout(150L, a100, Map.empty)
  )

  val highestPromisedHighestCommittedLens = progressLens andThen Progress.highestPromisedHighestCommitted

  val accepts98thru100 = Seq(a98, a99, a100)

  def journaled98thru100(logIndex: Long): Option[Accept] = {
    logIndex match {
      case 98L => Option(a98)
      case 99L => Option(a99)
      case 100L => Option(a100)
      case _ => None
    }
  }

  val identifier101: Identifier = Identifier(3, BallotNumber(3, 3), 101L)

  def journaled98thru101(logIndex: Long): Option[Accept] = {
    logIndex match {
      case 98L => Option(a98)
      case 99L => Option(a99)
      case 100L => Option(a100)
      case 101L => Option(a101)
      case _ => None
    }
  }

  val initialDataCommittedSlotOne = PaxosData(
    Progress(
      BallotNumber(lowValue, lowValue), Identifier(0, BallotNumber(lowValue, lowValue), 1)
    ), 0, 0, 3, TreeMap(), None, TreeMap(), Map.empty[Identifier, (CommandValue, DummyRemoteRef)])

  val a101 = Accept(identifier101, v3)

  val misorderedAccepts = Seq(a98, a99, a101, a100)

  val progress96 = Progress(BallotNumber(0, 0), Identifier(0, BallotNumber(0, 0), 96L))

  val progress97 = Progress(BallotNumber(0, 0), Identifier(0, BallotNumber(0, 0), 97L))

  val progress98 = Progress(BallotNumber(0, 0), Identifier(0, BallotNumber(0, 0), 98L))

  val zeroProgress = Progress(BallotNumber(0, 0), Identifier(0, BallotNumber(0, 0), 0L))

  def progressWith(promise: BallotNumber, committed: BallotNumber) = zeroProgress.copy(highestPromised = promise,
    highestCommitted = zeroProgress.highestCommitted.copy(number = committed))

  val timedOutAt100AcceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout] = TreeMap(
    a98.id -> AcceptResponsesAndTimeout(100L, a98, Map.empty),
    a99.id -> AcceptResponsesAndTimeout(50L, a99, Map.empty)
  )

  val recoverHighPrepare = Prepare(Identifier(0, BallotNumber(lowValue + 1, 0), 1L))

  val highPrepareEpoch = Some(recoverHighPrepare.id.number)
  val prepareSelfNack = SortedMap.empty[Identifier, Map[Int, PrepareResponse]] ++
    Seq((recoverHighPrepare.id -> Map(0 -> PrepareNack(recoverHighPrepare.id, 0, initialData.progress, 0, 0))))
  val selfNackPrepares = initialData.copy(clusterSize = 3, epoch = highPrepareEpoch, prepareResponses = prepareSelfNack, acceptResponses = SortedMap.empty)

  val prepareSelfAck = SortedMap.empty[Identifier, Map[Int, PrepareResponse]] ++
    Seq((recoverHighPrepare.id -> Map(0 -> PrepareAck(recoverHighPrepare.id, 0, initialData.progress, 0, 0, None))))
  val selfAckPrepares = initialData.copy(clusterSize = 3, epoch = highPrepareEpoch, prepareResponses = prepareSelfAck, acceptResponses = SortedMap.empty)

  val initialData97 = PaxosData[DummyRemoteRef](
    progress = Progress(
      highestPromised = BallotNumber(lowValue, lowValue),
      highestCommitted = Identifier(from = 0, number = BallotNumber(lowValue, lowValue), logIndex = 97)
    ),
    leaderHeartbeat = 0,
    timeout = 0,
    clusterSize = 3, prepareResponses = TreeMap(), epoch = None, acceptResponses = TreeMap(), clientCommands = Map.empty[Identifier, (CommandValue, DummyRemoteRef)])

  val initialData96 = initialData97.copy(progress = progress96)

  val a98ack0 = AcceptAck(a98.id, 0, initialData97.progress)
  val a98ack1 = AcceptAck(a98.id, 1, initialData97.progress)
  val a98ack3 = AcceptAck(a98.id, 3, initialData97.progress)

  val a98nack1 = AcceptNack(a98.id, 1, initialData97.progress)
  val a98nack2 = AcceptNack(a98.id, 2, initialData97.progress)

  val a99ack1 = AcceptAck(a99.id, 1, initialData97.progress)
  val a99ack0 = AcceptAck(a99.id, 0, initialData97.progress)

  val a98ackProgress98 = AcceptAck(a98.id, 1, progress98)

  val acceptSelfAck98 = SortedMap.empty[Identifier, AcceptResponsesAndTimeout] ++
    Seq((a98.id -> AcceptResponsesAndTimeout(Long.MaxValue, a98, Map(0 -> a98ack0))))

  val acceptSplitAckAndNack = SortedMap.empty[Identifier, AcceptResponsesAndTimeout] ++
    Seq((a98.id -> AcceptResponsesAndTimeout(Long.MaxValue, a98, Map(0 -> a98ack0, 1 -> a98nack1)))
    )

  val acceptkAndTwoNack = SortedMap.empty[Identifier, AcceptResponsesAndTimeout] ++
    Seq((a98.id -> AcceptResponsesAndTimeout(Long.MaxValue, a98, Map(0 -> a98ack0, 1 -> a98nack1, 2 -> a98nack2)))
    )

  val acceptSelfAck98and99 = SortedMap.empty[Identifier, AcceptResponsesAndTimeout] ++
    Seq((a98.id -> AcceptResponsesAndTimeout(Long.MaxValue, a98, Map(0 -> a98ack0))),
      (a99.id -> AcceptResponsesAndTimeout(Long.MaxValue, a99, Map(0 -> a99ack0))))

  val acceptAck98and99empty = SortedMap.empty[Identifier, AcceptResponsesAndTimeout] ++
    Seq((a98.id -> AcceptResponsesAndTimeout(Long.MaxValue, a98, Map(0 -> a98ack0))),
      (a99.id -> AcceptResponsesAndTimeout(Long.MaxValue, a99, Map.empty)))
}
