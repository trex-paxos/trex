package com.github.simbo1905.trex.internals

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorRef
import akka.testkit.{TestActorRef, TestKit}
import com.github.simbo1905.trex.internals.PaxosActor._
import com.github.simbo1905.trex.library._
import com.typesafe.config.ConfigFactory
import org.scalamock.scalatest.MockFactory
import org.scalatest.{OptionValues, Matchers}

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.postfixOps

import Ordering._

object NoopPaxosLogging extends PaxosLogging {
  override def info(msg: String): Unit = {}

  override def info(msg: String, one: Any): Unit = {}

  override def info(msg: String, one: Any, two: Any): Unit = {}

  override def info(msg: String, one: Any, two: Any, three: Any): Unit = {}

  override def info(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = {}

  override def debug(msg: String, one: Any, two: Any): Unit = {}

  override def debug(msg: String, one: Any, two: Any, three: Any): Unit = {}

  override def debug(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = {}

  override def error(msg: String): Unit = {}

  override def warning(msg: String): Unit = {}

  override def warning(msg: String, one: Any, two: Any): Unit = {}
}

class TestAcceptMapJournal extends Journal {
  var accept: Map[Long, Accept] = Map.empty

  def accept(accepted: Accept*): Unit = accepted foreach { a =>
    accept = accept + (a.id.logIndex -> a)
  }

  def accepted(logIndex: Long): Option[Accept] = {
    accept.get(logIndex)
  }

  def bounds: JournalBounds = {
    val keys = accept.keys
    if (keys.isEmpty) JournalBounds(0L, 0L) else JournalBounds(keys.head, keys.last)
  }

  var progress: Progress = null

  def load(): Progress = progress

  def save(p: Progress): Unit = progress = p
}

case class TimeAndParameter(time: Long, parameter: Any)

class TestTimingsFileJournal(storeFile: File, retained: Int) extends FileJournal(storeFile, retained) {

  var actionsWithTimestamp = Seq.empty[(String, TimeAndParameter)]

  super.save(Journal.minBookwork)

  override def save(progress: Progress): Unit = {
    actionsWithTimestamp = actionsWithTimestamp :+("save", TimeAndParameter(System.nanoTime(), progress))
    super.save(progress)
  }

  override def accept(a: Accept*): Unit = {
    actionsWithTimestamp = actionsWithTimestamp :+("accept", TimeAndParameter(System.nanoTime(), a))
    super.accept(a: _*)
  }

  override protected def init(): Unit = {
    // dont' call init which invokes save in parent constructor call in subclass constructor
  }
}

class DelegatingJournal(val inner: Journal) extends Journal {
  override def save(progress: Progress): Unit = inner.save(progress)

  override def bounds: JournalBounds = inner.bounds

  override def load(): Progress = inner.load()

  override def accepted(logIndex: Long): Option[Accept] = inner.accepted(logIndex)

  override def accept(a: Accept*): Unit = inner.accept(a: _*)
}

class TestPaxosIO extends PaxosIO[ActorRef] {
  override def plog: PaxosLogging = NoopPaxosLogging

  override def randomTimeout: Long = 0

  override def clock: Long = 0

  override def deliver(value: CommandValue): Any = {}

  override def journal: Journal = throw new AssertionError("deliberately not implemented")

  override def respond(ref: ActorRef, data: Any): Unit = {}

  override def send(msg: PaxosMessage): Unit = {}

  override def sendNoLongerLeader(clientCommands: Map[Identifier, (CommandValue, ActorRef)]): Unit = {}

  override def minPrepare: Prepare = throw new AssertionError("deliberately not implemented")

  override def sender: ActorRef = throw new AssertionError("deliberately not implemented")
}

object AllStateSpec {
  val config = ConfigFactory.parseString("trex.leader-timeout-min=10\ntrex.leader-timeout-max=20\nakka.loglevel = \"DEBUG\"\nakka.actor.serialize-messages=on")

  val expectedString = "Knossos"
  val expectedBytes = expectedString.getBytes

  val lowValue = Int.MinValue + 1

  val minIdentifier = Identifier(from = 0, number = BallotNumber(lowValue, lowValue), logIndex = Long.MinValue)

  val initialData = PaxosData[ActorRef](
    progress = Progress(
      highestPromised = BallotNumber(lowValue, lowValue),
      highestCommitted = Identifier(from = 0, number = BallotNumber(lowValue, lowValue), logIndex = 0)
    ),
    leaderHeartbeat = 0,
    timeout = 0,
    clusterSize = 3, prepareResponses = TreeMap(), epoch = None, acceptResponses = TreeMap(), clientCommands = Map.empty[Identifier, (CommandValue, ActorRef)])

  val minute = 1000 * 60 // ms

  def noDelivery(value: CommandValue): Array[Byte] = throw new AssertionError("deliberately not implemented")

  val atomicCounter = new AtomicInteger()

  def tempRecordTimesFileJournal = new TestTimingsFileJournal(File.createTempFile(s"db${this.getClass.getSimpleName}${AllStateSpec.atomicCounter.getAndIncrement}", "tmp"), 100)
}

trait AllStateSpec {
  self: TestKit with MockFactory with Matchers with OptionValues =>

  import AllStateSpec._
  import PaxosActor.Configuration

  /**
   * Bugs can leak messages from one test to the next this slow check will tell you which test has a leak if you enable this check with a system property
   */
  def checkForLeakedMessages = {
    if( java.lang.Boolean.getBoolean("checkForLeakedMessages")) expectNoMsg(100 millisecond)
  }

  val leaderHeartbeat2 = 2
  val clusterSize3 = 3
  val clusterSize5 = 5
  val timeout4 = 4

  def ignoresCommitLessThanLast(state: PaxosRole)(implicit sender: ActorRef) {
    val stubJournal: Journal = stub[Journal]
    // given a candidate with no responses
    val identifier = Identifier(0, BallotNumber(lowValue + 1, 0), 1L)
    val lessThan = Identifier(0, BallotNumber(lowValue, 0), 0L)
    val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, stubJournal, ArrayBuffer.empty, None))
    fsm.underlyingActor.setAgent(state, initialData.copy(clusterSize = 3, progress = initialData.progress.copy(highestCommitted = identifier)))
    // when it gets commit
    val commit = Commit(lessThan)
    fsm ! commit
    // it sends no messages
    expectNoMsg(25 millisecond)
    // and stays a candidate
    assert(fsm.underlyingActor.role == state)
  }

  def ackAccept(state: PaxosRole)(implicit sender: ActorRef) {
    val stubJournal: Journal = stub[Journal]
    // given initial state
    val promised = BallotNumber(6, 1)
    val initialData = PaxosData(Progress(promised, minIdentifier), leaderHeartbeat2, timeout4, clusterSize3, TreeMap(), None, TreeMap(), Map.empty[Identifier, (CommandValue, ActorRef)])
    val identifier = Identifier(0, promised, 1)
    val accepted = Accept(identifier, ClientRequestCommandValue(0, expectedBytes))
    // when our node sees the accept message
    val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, stubJournal, ArrayBuffer.empty, None))
    fsm.underlyingActor.setAgent(state, initialData)
    fsm ! accepted
    // then it accepts
    expectMsg(250 millisecond, AcceptAck(identifier, 0, initialData.progress))
    // stays in the same state
    assert(fsm.underlyingActor.role == state)
    // does not update its data
    assert(fsm.underlyingActor.data == initialData)
    // journals the new value
    (stubJournal.accept _).verify(Seq(accepted))
  }

  // http://stackoverflow.com/q/29880949/329496
  def ackHigherAcceptMakingPromise(state: PaxosRole)(implicit sender: ActorRef): Unit = {
    // given initial state promised to node 1 count 6
    val promised = BallotNumber(6, 1)
    val initialData = PaxosData(Progress(promised, minIdentifier), leaderHeartbeat2, timeout4, clusterSize3, TreeMap(), None, TreeMap(), Map.empty[Identifier, (CommandValue, ActorRef)])
    // and a journal which records the time save was called
    val testJournal = AllStateSpec.tempRecordTimesFileJournal

    var sendTs = 0L
    val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, testJournal, ArrayBuffer.empty, None) {
      override def send(actor: ActorRef, msg: Any): Unit = {
        sendTs = System.nanoTime()
        super.send(actor, msg)
      }
    })
    fsm.underlyingActor.setAgent(state, initialData)

    // when our node sees an accept message for using a higher number
    val higherNumber = BallotNumber(7, 2)
    val identifier = Identifier(0, higherNumber, 1)
    val accepted = Accept(identifier, ClientRequestCommandValue(0, expectedBytes))
    fsm ! accepted

    // then it accepts
    expectMsg(250 millisecond, AcceptAck(identifier, 0, initialData.progress))
    // stays in the same state
    assert(fsm.underlyingActor.role == state)
    // journals the new accepts
    val actionsWithTimestamp = testJournal.actionsWithTimestamp.toMap
    val TimeAndParameter(_, accepts: Seq[Any]) = actionsWithTimestamp.get("accept").getOrElse(fail)
    assert(accepts == Seq(accepted))
    // journals the new progress
    val TimeAndParameter(saveTs, progress: Progress) = actionsWithTimestamp.get("save").getOrElse(fail)
    progress match {
      case p if p.highestPromised == higherNumber => // good
      case x => fail(x.toString)
    }
    // updates its promise to the new value
    assert(fsm.underlyingActor.data == initialData.copy(progress = initialData.progress.copy(highestPromised = higherNumber)))
    // and the timestamps show that the save happened before the send
    saveTs should not be 0L
    sendTs should not be 0L
    (saveTs < sendTs) shouldBe true
  }

  def ackDuplicatedAccept(state: PaxosRole)(implicit sender: ActorRef) {
    val stubJournal: Journal = stub[Journal]
    // given initial state
    val promised = BallotNumber(6, 1)
    val initialData = PaxosData(Progress(promised, Identifier(0, promised, Long.MinValue)), leaderHeartbeat2, timeout4, clusterSize3, TreeMap(), None, TreeMap(), Map.empty[Identifier, (CommandValue, ActorRef)])
    val identifier = Identifier(0, promised, 1)
    // and some already journalled accept
    val accepted = Accept(identifier, ClientRequestCommandValue(0, expectedBytes))
    stubJournal.accepted _ when 0L returns Some(accepted)
    // when our node sees this
    val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, stubJournal, ArrayBuffer.empty, None))
    fsm.underlyingActor.setAgent(state, initialData)
    fsm ! accepted
    // then it accepts
    expectMsg(250 millisecond, AcceptAck(identifier, 0, initialData.progress))
    // stays in the same state
    assert(fsm.underlyingActor.role == state)
    // does not update its data
    assert(fsm.underlyingActor.data == initialData)
    // journals the new value
    (stubJournal.accept _).verify(Seq(accepted))
  }

  def nackAcceptAboveCommitWatermark(state: PaxosRole)(implicit sender: ActorRef) {
    val stubJournal: Journal = stub[Journal]
    // given initial state
    val committedLogIndex = 1
    val promised = BallotNumber(5, 0)
    val initialData = PaxosData(Progress(promised, Identifier(0, promised, committedLogIndex)), leaderHeartbeat2, timeout4, clusterSize3, TreeMap(), None, TreeMap(), Map.empty[Identifier, (CommandValue, ActorRef)])
    val higherIdentifier = Identifier(0, BallotNumber(6, 0), committedLogIndex)
    val acceptedAccept = Accept(higherIdentifier, ClientRequestCommandValue(0, expectedBytes))
    // and some duplicated accept
    // when our node sees this
    val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, stubJournal, ArrayBuffer.empty, None))
    fsm.underlyingActor.setAgent(state, initialData)
    fsm ! acceptedAccept
    // then it does not respond
    expectMsg(250 millisecond, AcceptNack(higherIdentifier, 0, initialData.progress))
    // stays in the same state
    assert(fsm.underlyingActor.role == state)
    // does not update its data
    assert(fsm.underlyingActor.data == initialData)
  }

  def nackAcceptLowerThanPromise(state: PaxosRole)(implicit sender: ActorRef) {
    val stubJournal: Journal = stub[Journal]
    // given initial state
    val promised = BallotNumber(5, 1)
    val initialData = PaxosData(Progress(promised, minIdentifier), leaderHeartbeat2, 0, clusterSize3, TreeMap(), None, TreeMap(), Map.empty[Identifier, (CommandValue, ActorRef)])
    val lowerIdentifier = Identifier(0, BallotNumber(4, 2), 1)
    val rejectedAccept = Accept(lowerIdentifier, ClientRequestCommandValue(0, expectedBytes))
    // and some duplicated accept
    // when our node sees this
    val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, stubJournal, ArrayBuffer.empty, None))
    fsm.underlyingActor.setAgent(state, initialData)
    fsm ! rejectedAccept
    // then it does not respond
    expectMsg(250 millisecond, AcceptNack(lowerIdentifier, 0, initialData.progress))
    // stays in the same state
    assert(fsm.underlyingActor.role == state)
    // does not update its data
    assert(fsm.underlyingActor.data == initialData)
  }

  def journalsButDoesNotCommitIfNotContiguousRetransmissionResponse(state: PaxosRole)(implicit sender: ActorRef) = {
    // given some retransmitted committed values
    val v1 = ClientRequestCommandValue(11, Array[Byte] {
      0
    })
    val v2 = ClientRequestCommandValue(22, Array[Byte] {
      1
    })
    val v3 = ClientRequestCommandValue(33, Array[Byte] {
      2
    })
    val a1 =
      Accept(Identifier(1, BallotNumber(1, 1), 98L), v1)
    val a2 =
      Accept(Identifier(2, BallotNumber(2, 2), 99L), v2)
    val a3 =
      Accept(Identifier(3, BallotNumber(3, 3), 100L), v3)
    val retransmission = RetransmitResponse(1, 0, Seq(a1, a2, a3), Seq.empty)

    // and an node that has committed to not just prior to those values such that it cannot in-order deliver
    val lastCommitted = Identifier(1, BallotNumber(1, 1), 96L)
    val oldProgress = Progress.highestPromisedHighestCommitted.set(initialData.progress, (lastCommitted.number, lastCommitted))
    val fileJournal: FileJournal = AllStateSpec.tempRecordTimesFileJournal
    val delivered = ArrayBuffer[CommandValue]()
    val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, fileJournal, delivered, None))
    fsm.underlyingActor.setAgent(state, initialData.copy(progress = oldProgress))

    // when the retransmission is received
    fsm ! retransmission

    // it sends no messages
    expectNoMsg(25 milliseconds)
    // stays in state
    assert(fsm.underlyingActor.role == state)
    // does not update its commit index
    assert(fsm.underlyingActor.data.progress.highestCommitted.logIndex == 96L)
    // does update highest promise
    assert(fsm.underlyingActor.data.progress.highestPromised == a3.id.number)
    // does not delivered any committed values
    delivered.size should be(0)
    // does journal the values
    fileJournal.accepted(98L).value.id should be(a1.id)
    fileJournal.accepted(99L).value.id should be(a2.id)
    fileJournal.accepted(100L).value.id should be(a3.id)
    // does journal the new promise
    fileJournal.load() match {
      case Progress(a3.id.number, lastCommitted) => // good
    }
  }

  def journalsAcceptMessagesAndSetsHigherPromise(state: PaxosRole)(implicit sender: ActorRef) = {
    // given some retransmitted committed values
    val v1 = ClientRequestCommandValue(11, Array[Byte] {
      0
    })
    val v2 = ClientRequestCommandValue(22, Array[Byte] {
      1
    })
    val v3 = ClientRequestCommandValue(33, Array[Byte] {
      2
    })
    val a1 =
      Accept(Identifier(1, BallotNumber(1, 1), 98L), v1)
    val a2 =
      Accept(Identifier(2, BallotNumber(2, 2), 99L), v2)
    val a3 =
      Accept(Identifier(3, BallotNumber(3, 3), 100L), v3)
    val retransmission = RetransmitResponse(1, 0, Seq.empty, Seq(a1, a2, a3))

    // and an node that has committed to not just prior to those values
    val lastCommitted = Identifier(1, BallotNumber(1, 1), 97L)
    val oldProgress = Progress.highestPromisedHighestCommitted.set(initialData.progress, (lastCommitted.number, lastCommitted))
    val fileJournal: FileJournal = AllStateSpec.tempRecordTimesFileJournal
    val delivered = ArrayBuffer[CommandValue]()
    val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, fileJournal, delivered, None))
    fsm.underlyingActor.setAgent(state, initialData.copy(progress = oldProgress))

    // when the retransmission is received
    fsm ! retransmission

    // it sends no messages
    expectNoMsg(25 milliseconds)
    // stays in state
    assert(fsm.underlyingActor.role == state)
    // does not update its commit index
    assert(fsm.underlyingActor.data.progress.highestCommitted == lastCommitted)
    // does update highest promise
    assert(fsm.underlyingActor.data.progress.highestPromised == a3.id.number)
    // does not delivered any committed values
    delivered.size should be(0)
    // does journal the values
    fileJournal.accepted(98L).value.id should be(a1.id)
    fileJournal.accepted(99L).value.id should be(a2.id)
    fileJournal.accepted(100L).value.id should be(a3.id)
    // does journal the new promise
    fileJournal.load() match {
      case Progress(a3.id.number, lastCommitted) => // good
    }
  }

  def ackRepeatedPrepare(state: PaxosRole)(implicit sender: ActorRef) {
    // given no previous value
    val stubJournal: Journal = stub[Journal]
    stubJournal.accepted _ when 1L returns None
    // given higher initial state
    val high = BallotNumber(10, 1)
    val initialData = PaxosData(Progress(high, minIdentifier), leaderHeartbeat2, timeout4, clusterSize3, TreeMap(), None, TreeMap(), Map.empty[Identifier, (CommandValue, ActorRef)])
    // and same prepare
    val highIdentifier = Identifier(0, high, 1L)
    val highPrepare = Prepare(highIdentifier)
    // and an empty accept journal
    stubJournal.bounds _ when() returns minJournalBounds
    // when our node sees this
    val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, stubJournal, ArrayBuffer.empty, None))
    fsm.underlyingActor.setAgent(state, initialData)
    fsm ! highPrepare
    // then it responds with a ack
    expectMsg(250 millisecond, PrepareAck(highIdentifier, 0, initialData.progress, Long.MinValue, leaderHeartbeat2, None))
    // stays in the same state
    assert(fsm.underlyingActor.role == state)
    // does not update its data
    assert(fsm.underlyingActor.data == initialData)
    // and checks the journal
    (stubJournal.accepted _).verify(1L)
  }

  def ackHigherPrepare(state: PaxosRole)(implicit sender: ActorRef) = {
    // given no previous value
    val testJournal = AllStateSpec.tempRecordTimesFileJournal
    // given low initial state
    val low = BallotNumber(5, 1)
    val initialData = PaxosData(Progress(low, minIdentifier), leaderHeartbeat2, timeout4, clusterSize3, TreeMap(), None, TreeMap(), Map.empty[Identifier, (CommandValue, ActorRef)])
    // and same prepare
    val high = BallotNumber(10, 2)
    val highIdentifier = Identifier(0, high, 1L)
    val highPrepare = Prepare(highIdentifier)
    var sendTs = 0L
    // when our node sees this
    val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, testJournal, ArrayBuffer.empty, None) {
      override def send(actor: ActorRef, msg: Any): Unit = {
        sendTs = System.nanoTime()
        super.send(actor, msg)
      }
    })
    fsm.underlyingActor.setAgent(state, initialData)
    fsm ! highPrepare
    // then it responds with a ack
    expectMsg(250 millisecond, PrepareAck(highIdentifier, 0, initialData.progress.copy(highestPromised = high), Long.MinValue, leaderHeartbeat2, None))
    // it must be a follower as it can no longer journal client data so cannot commit them so cannot lead
    assert(fsm.underlyingActor.role == Follower)
    // it must have upated its highest promised
    assert(fsm.underlyingActor.data.progress.highestPromised == high)
    // journals the new progress
    val actionsWithTimestamp = testJournal.actionsWithTimestamp.toMap
    val TimeAndParameter(saveTs, progress: Progress) = actionsWithTimestamp.get("save").getOrElse(fail)
    progress match {
      case p if p == fsm.underlyingActor.data.progress => // good
      case x => fail(x.toString)
    }
    // and the timestamps show that the save happened before the send
    saveTs should not be 0L
    sendTs should not be 0L
    (saveTs < sendTs) shouldBe true
    fsm
  }

}