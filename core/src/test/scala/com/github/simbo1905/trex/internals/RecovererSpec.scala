package com.github.simbo1905.trex.internals

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{DefaultTimeout, ImplicitSender, TestActorRef, TestKit}
import com.github.simbo1905.trex.library._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfter, Matchers, OptionValues, WordSpecLike}

import scala.collection.immutable.SortedMap
import scala.collection.mutable.ArrayBuffer
import scala.compat.Platform
import scala.concurrent.duration._
import scala.language.postfixOps

class RecovererSpec
  extends TestKit(ActorSystem("RecovererSpec", AllStateSpec.config))
  with DefaultTimeout with WordSpecLike with Matchers with MockFactory with ImplicitSender
  with BeforeAndAfter with AllStateSpec with LeaderLikeSpec with NotLeaderSpec with OptionValues with PaxosLenses[ActorRef] {

  import AllStateSpec._
  import Ordering._
  import PaxosActor._

  val recoverHighPrepare = Prepare(Identifier(0, BallotNumber(lowValue + 1, 0), 1L))

  val otherHigherPrepare = Prepare(Identifier(2, BallotNumber(lowValue + 1, 2), 1L))

  "Recoverer" should {
    "respond to client data by saying that you are not the leader" in {
      respondsToClientDataBySayingNotTheLeader(Recoverer)
    }
    "ack a repeated prepare" in {
      ackRepeatedPrepare(Recoverer)
    }
    "accept higher prepare" in {
      ackHigherPrepare(Recoverer)
    }
    journalsButDoesNotCommitIfNotContiguousRetransmissionResponse(Recoverer)
  }
  "journals accept messages and sets higher promise" in {
    journalsAcceptMessagesAndSetsHigherPromise(Recoverer)
  }
  "nack an accept lower than its last promise" in {
    nackAcceptLowerThanPromise(Recoverer)
  }
  "nack an accept for a slot which is committed" in {
    nackAcceptAboveCommitWatermark(Recoverer)
  }
  "ack duplidated accept" in {
    ackDuplicatedAccept(Recoverer)
  }
  "journals accepted message" in {
    ackAccept(Recoverer)
  }
  "increments promise with higher accept" in {
    ackHigherAcceptMakingPromise(Recoverer)
  }
  "ignore commit message for lower log index" in {
    ignoreCommitMessageLogIndexLessThanLastCommit(Recoverer)
  }
  "ignore commit message equal than last committed lower nodeIdentifier" in {
    ignoreCommitMessageSameSlotLowerNodeIdentifier(Recoverer)
  }
  "backdown to follower on a commit of same slot but with higher node number" in {
    backdownToFollowerOnCommitSameSlotHigherNodeIdentifier(Recoverer)
  }
  "backdown to follower and request retransmission on commit higher than last committed" in {
    backdownToFollowerAndRequestRetransmissionOnCommitHigherThanLastCommitted(Recoverer)
  }
  "backdown to follower and perform commit" in {
    backdownToFollowerAndCommitOnCommitHigherThanLastCommitted(Recoverer)
  }

  "fix a no-op and promote to Leader then commits if in a three node cluster gets a majority with one ack with no values to fix" in {
    // given a recoverer with self vote
    val timenow = 999L
    val (fsm, prepareId) = recovererNoResponsesInClusterOfSize(3, timenow)
    val accept = Accept(prepareId, NoOperationCommandValue)
    // when a majority prepare response with an ack from node1
    val ack1 = PrepareAck(prepareId, 1, initialData.progress, 0, 0, None)
    fsm ! ack1
    // it boardcasts a no-op
    expectMsg(100 millisecond, accept)
    // and becomes leader
    assert(fsm.underlyingActor.role == Leader)

    // when a majority accept response with an ack from node1
    fsm ! AcceptAck(prepareId, 1, initialData.progress)
    // it commits the no-op
    expectMsgPF(100 millisecond) {
      case Commit(prepareId, _) => Unit
    }
    // and it has the epoch
    assert(fsm.underlyingActor.data.epoch == Some(prepareId.number))
    // and it has cleared the recover votes
    assert(fsm.underlyingActor.data.prepareResponses.isEmpty)
    // and sets a fresh timeout
    fsm.underlyingActor.data.timeout shouldBe 1234L
    // and send happens after save
    assert(saveTime > 0L && sendTime > 0L && saveTime < sendTime)
  }

  "backs down if it has to make a higher promise" in {
    // given a recoverer with self vote
    val (fsm, prepareId) = recovererNoResponsesInClusterOfSize(3)
    val accept = Accept(prepareId, NoOperationCommandValue)
    // which makes a promise to another leader
    fsm ! otherHigherPrepare
    expectMsgPF(100 millisecond) {
      case a: PrepareAck => // good
    }
    // and backs down to a follower as it cannot accept client values under its own epoch it cannot journal them so cannot commit so cannot lead
    assert(fsm.underlyingActor.role == Follower)
  }

  "promote to Leader and ack its own accept if it has not made a higher promise" in {
    // given a recoverer with self vote
    val (fsm, prepareId) = recovererNoResponsesInClusterOfSize(3)
    val accept = Accept(prepareId, NoOperationCommandValue)
    // when a majority prepare response with an ack from node1
    val ack1 = PrepareAck(prepareId, 1, initialData.progress, 0, 0, None)
    fsm ! ack1
    // it boardcasts a no-op
    expectMsg(100 millisecond, accept)
    // and becomes leader
    assert(fsm.underlyingActor.role == Leader)
    // and has acked its own accept
    fsm.underlyingActor.data.acceptResponses match {
      case map if map.isEmpty =>
        fail
      case map =>
        map.get(accept.id) match {
          case None =>
            fail
          case Some(AcceptResponsesAndTimeout(_, _, responses)) =>
            responses.values.headOption match {
              case Some(a: AcceptAck) => // good
              case x =>
                fail(x.toString)
            }
        }
    }
  }

  "fix a no-op promote to Leader and commits if in a five node cluster gets a majority with two acks with no values to fix" in {
    // given a recoverer with no responses
    val timenow = 999L
    val (fsm, prepareId) = recovererNoResponsesInClusterOfSize(5, timenow)
    // when a majority prepare response with an ack from node1 and node2
    val ack1 = PrepareAck(prepareId, 1, initialData.progress, 0, 0, None)
    fsm ! ack1
    val ack2 = PrepareAck(prepareId, 2, initialData.progress, 0, 0, None)
    fsm ! ack2
    // it boardcasts a no-op
    expectMsg(100 millisecond, Accept(prepareId, NoOperationCommandValue))
    // and saves before it sends
    assert(saveTime != 0L && sendTime != 0L && saveTime < sendTime)
    saveTime = 0L
    sendTime = 0L
    // when a majority accept response with an ack from node1 and node2
    fsm ! AcceptAck(prepareId, 1, initialData.progress)
    fsm ! AcceptAck(prepareId, 2, initialData.progress)
    // it commits the no-op
    expectMsgPF(100 millisecond) {
      case Commit(prepareId, _) => Unit
    }
    // and becomes leader
    assert(fsm.underlyingActor.role == Leader)
    // and it has the epoch
    assert(fsm.underlyingActor.data.epoch == Some(prepareId.number))
    // and it has cleared the recover votes
    assert(fsm.underlyingActor.data.prepareResponses.isEmpty)
    // and sets a fresh timeout
    fsm.underlyingActor.data.timeout shouldBe 1234L
    // and it saves before it sends
    assert(saveTime != 0L && sendTime != 0L && saveTime < sendTime)
  }

  "fix a high value and promote to Leader then commits if in a three node cluster gets a majority with one ack" in {
    // given a recoverer with self vote
    val timenow = 999L
    val (fsm, prepareId) = recovererNoResponsesInClusterOfSize(3, timenow)
    // and some value returned in the promise from node1 with some lower number
    val lowerId = prepareId.copy(number = prepareId.number.copy(counter = prepareId.number.counter - 1))
    val ack1 = PrepareAck(prepareId, 1, initialData.progress, 0, 0, Some(Accept(lowerId, ClientRequestCommandValue(0, expectedBytes))))
    fsm ! ack1
    // it boardcasts the payload from the promise under its higher epoch number
    expectMsg(100 millisecond, Accept(prepareId, ClientRequestCommandValue(0, expectedBytes)))
    // and becomes leader
    assert(fsm.underlyingActor.role == Leader)
    // when a majority accept response with an ack from node1
    fsm ! AcceptAck(prepareId, 1, initialData.progress)
    // it commits the accept
    assert(fsm.underlyingActor.delivered.head == ClientRequestCommandValue(0, expectedBytes))
    expectMsgPF(100 millisecond) {
      case Commit(prepareId, _) => Unit
    }
    // and it has the epoch
    assert(fsm.underlyingActor.data.epoch == Some(prepareId.number))
    // and it has cleared the recover votes
    assert(fsm.underlyingActor.data.prepareResponses.isEmpty)
    // and sets a fresh timeout
    fsm.underlyingActor.data.timeout shouldBe 1234L
  }

  "fix a high value promote to Leader then commits if in a five node cluster gets a majority with two acks" in {
    saveTime = 0L
    sendTime = 0L
    val timenow = 999L
    // given a recoverer with no responses
    val (fsm, prepareId) = recovererNoResponsesInClusterOfSize(5, timenow)
    // when a majority prepare response with an ack from node1 and node2 with some value in the promise from node2
    val ack1 = PrepareAck(prepareId, 1, initialData.progress, 0, 0, None)
    fsm ! ack1
    val lowerId = prepareId.copy(number = prepareId.number.copy(counter = prepareId.number.counter - 1))
    val ack2 = PrepareAck(prepareId, 2, initialData.progress, 0, 0, Some(Accept(lowerId, ClientRequestCommandValue(0, expectedBytes))))
    fsm ! ack2
    // it boardcasts the payload from the promise under its higher epoch number
    expectMsg(100 millisecond, Accept(prepareId, ClientRequestCommandValue(0, expectedBytes)))
    // its saves before it sends
    assert(saveTime > 0L && sendTime > 0L && saveTime < sendTime)
    saveTime = 0L
    sendTime = 0L
    // when a majority accept response with an ack from node1 and node2
    fsm ! AcceptAck(prepareId, 1, initialData.progress)
    fsm ! AcceptAck(prepareId, 2, initialData.progress)
    // it commits the accept
    assert(fsm.underlyingActor.delivered.head == ClientRequestCommandValue(0, expectedBytes))
    expectMsgPF(100 millisecond) {
      case Commit(prepareId, _) => Unit
    }
    // and has become leader
    assert(fsm.underlyingActor.role == Leader)
    // and it has the same epoch
    assert(fsm.underlyingActor.data.epoch == Some(prepareId.number))
    // and it has cleared the recover votes
    assert(fsm.underlyingActor.data.prepareResponses.isEmpty)
    // and it has cleared the accept votes its own accept
    assert(fsm.underlyingActor.data.acceptResponses.isEmpty)
    // and sets a fresh timeout
    fsm.underlyingActor.data.timeout shouldBe 1234L
    // and send happens after save
    assert(saveTime > 0L && sendTime > 0L && saveTime < sendTime)

  }

  "requests retransmission if is behind when gets a majority showing others have higher commit watermark" in {
    // given a recoverer with self vote
    val (fsm, prepareId) = recovererNoResponsesInClusterOfSize(3)
    // when a majority prepare response with an ack from node1 which shows it is behind
    val higherProgress = initialData.progress.copy(highestCommitted = initialData.progress.highestCommitted.copy(logIndex = 5L))
    val ack1 = PrepareAck(prepareId, 1, higherProgress, 0, 0, None)
    fsm ! ack1
    // it request retransmission
    expectMsg(100 millisecond, RetransmitRequest(0, 1, 0L))
    // and sends the accept for the majority response
    expectMsg(100 millisecond, Accept(prepareId, NoOperationCommandValue))
  }

  "issue new prepares if it learns from the majority that other nodes have higher accepted values" in {
    saveTime = 0L
    sendTime = 0L
    // given a recoverer with self vote
    val (fsm, prepareId) = recovererNoResponsesInClusterOfSize(3)
    val accept = Accept(prepareId, NoOperationCommandValue)
    // when a majority prepare response with an ack from node1 which shows it has missed values
    val ack1 = PrepareAck(prepareId, 1, initialData.progress, 3, 0, None)
    fsm ! ack1
    // it boardcasts prepare messages for the missing slots and one slot beyond
    val identifier2 = prepareId.copy(logIndex = 2L)
    val prepare2 = Prepare(identifier2)
    expectMsg(100 millisecond, prepare2)
    val identifier3 = prepareId.copy(logIndex = 3L)
    val prepare3 = Prepare(identifier3)
    expectMsg(100 millisecond, prepare3)
    // and sends the accept for the majority response
    expectMsg(100 millisecond, Accept(prepareId, NoOperationCommandValue))
    // it says as follower
    assert(fsm.underlyingActor.role == Recoverer)
    // and makes self promises to the new prepare
    fsm.underlyingActor.data.prepareResponses match {
      case map if map.isEmpty =>
        fail
      case map =>
        assert(map.size == 2)
        val keys = map.keys.toSeq
        assert(keys.contains(identifier2))
        assert(keys.contains(identifier3))
    }
    // and accepts its own values
    fsm.underlyingActor.data.acceptResponses match {
      case accepts if accepts.isEmpty =>
        fail
      case accepts =>
        val keys = accepts.keys.toSeq
        assert(keys.contains(prepareId))
    }
    // and send happens after save
    assert(saveTime > 0L && sendTime > 0L && saveTime < sendTime)

  }

  "backs down with a majority negative prepare response" in {
    // given a recoverer with no responses
    val (fsm, prepareId) = recovererNoResponsesInClusterOfSize(3)
    // when a minority prepare response with an nack
    fsm ! PrepareNack(prepareId, 1, initialData.progress, 0, 0)
    fsm ! PrepareNack(prepareId, 2, initialData.progress, 0, 0)
    // it returns to follower
    fsm.underlyingActor.role should be(Follower)
    // with cleared state
    fsm.underlyingActor.data.prepareResponses.size should be(0)
    fsm.underlyingActor.data.epoch shouldBe None
    // and sends no message
    expectNoMsg(25 millisecond)
  }

  "reboardcast prepares if it gets a timeout with no majority response" in {
    // given a recoverer with no responses
    val (fsm, prepareId) = recovererNoResponsesInClusterOfSize(5, Long.MaxValue)
    // when a minority prepare response with an ack from node1
    val ack1 = PrepareAck(prepareId, 1, initialData.progress, 0, 0, None)
    fsm ! ack1
    // when our node gets a timeout
    fsm ! CheckTimeout
    // it reboardcasts the accept message
    expectMsg(100 millisecond, recoverHighPrepare)
  }

  "reissue same accept messages it gets a timeout and no challenge" in {
    resendsSameAcceptOnTimeoutNoOtherInfo(Recoverer)
  }

  "reissue higher accept messages upon learning of another nodes higher promise in a nack" in {
    resendsHigherAcceptOnLearningOtherNodeHigherPromise(Recoverer)
  }

  "reissues higher accept message upon having made a higher promise itself by the timeout" in {
    resendsHigherAcceptOnHavingMadeAHigherPromiseAtTimeout(Recoverer)
  }

  var sendTime = 0L
  var saveTime = 0L

  def recovererNoResponsesInClusterOfSize(numberOfNodes: Int, timenow: Long = Platform.currentTime) = {

    val prepareSelfVotes = SortedMap.empty[Identifier, Map[Int, PrepareResponse]] ++
      Seq((recoverHighPrepare.id -> Map(0 -> PrepareAck(recoverHighPrepare.id, 0, initialData.progress, 0, 0, None))))

    val data = initialData.copy(clusterSize = numberOfNodes, epoch = Some(recoverHighPrepare.id.number), prepareResponses = prepareSelfVotes, acceptResponses = SortedMap.empty)
    val fsm = TestActorRef(new TestPaxosActor(Configuration(config, numberOfNodes), 0, self, new TestAcceptMapJournal {
      override def save(p: Progress): Unit = {
        saveTime = System.nanoTime()
        super.save(p)
      }

      override def accept(accepted: Accept*): Unit = {
        saveTime = System.nanoTime()
        super.accept(accepted: _*)
      }
    }, ArrayBuffer.empty, None) {
      override def highestAcceptedIndex = 1L

      override def freshTimeout(l: Long) = 1234L

      override def clock() = timenow

      override def broadcast(msg: PaxosMessage): Unit = {
        sendTime = System.nanoTime()
        super.broadcast(msg)
      }
    })
    fsm.underlyingActor.setAgent(Recoverer, data)
    (fsm, recoverHighPrepare.id)
  }
}
