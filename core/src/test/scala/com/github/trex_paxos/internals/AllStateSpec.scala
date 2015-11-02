package com.github.trex_paxos.internals

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorRef
import akka.testkit.{TestActorRef, TestKit}
import com.github.trex_paxos.library._
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

class TestPaxosIO extends PaxosIO {
  override def plog: PaxosLogging = NoopPaxosLogging

  override def randomTimeout: Long = 0

  override def clock: Long = 0

  override def deliver(value: CommandValue): Any = {}

  override def journal: Journal = throw new AssertionError("deliberately not implemented")

  override def respond(ref: String, data: Any): Unit = {}

  override def send(msg: PaxosMessage): Unit = {}

  override def sendNoLongerLeader(clientCommands: Map[Identifier, (CommandValue, String)]): Unit = {}

  override def minPrepare: Prepare = throw new AssertionError("deliberately not implemented")

  override def senderId: String = throw new AssertionError("deliberately not implemented")
}

object AllStateSpec {
  val config = ConfigFactory.parseString("trex.leader-timeout-min=10\ntrex.leader-timeout-max=20\nakka.loglevel = \"DEBUG\"\nakka.actor.serialize-messages=on")

  val expectedString = "Knossos"
  val expectedBytes = expectedString.getBytes

  val lowValue = Int.MinValue + 1

  val minIdentifier = Identifier(from = 0, number = BallotNumber(lowValue, lowValue), logIndex = Long.MinValue)

  val initialData = PaxosData(
    progress = Progress(
      highestPromised = BallotNumber(lowValue, lowValue),
      highestCommitted = Identifier(from = 0, number = BallotNumber(lowValue, lowValue), logIndex = 0)
    ),
    leaderHeartbeat = 0,
    timeout = 0,
    clusterSize = 3, prepareResponses = TreeMap(), epoch = None, acceptResponses = TreeMap(), clientCommands = Map.empty[Identifier, (CommandValue, String)])

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



}