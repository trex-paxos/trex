package com.github.trex_paxos.library

import org.scalatest.{Matchers, OptionValues, WordSpecLike}

import scala.collection.immutable.SortedMap
import scala.collection.mutable.ArrayBuffer
import Ordering._

case class TimeAndMessage(message: Any, time: Long)

class TestPrepareResponseHandlerNoRetransmission extends PrepareResponseHandler with BackdownAgent {
  override def requestRetransmissionIfBehind(io: PaxosIO, agent: PaxosAgent, from: Int, highestCommitted: Identifier): Unit = {}
}

class PrepareResponseHandlerTests extends WordSpecLike with Matchers with OptionValues {

  import TestHelpers._

  val emptyJournal = new UndefinedJournal {
    override def accepted(logIndex: Long): Option[Accept] = None

    override def accept(a: Accept*): Unit = {}
  }

  "PrepareResponseHandler" should {
    "ignore a response that it is not awaiting" in {
      // given
      val handler = new TestPrepareResponseHandlerNoRetransmission
      val vote = PrepareAck(Identifier(1, BallotNumber(2, 3), 4L), 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), 12, 13, None)
      val PaxosAgent(_, role, state, _) = handler.handlePrepareResponse(new TestIO(new UndefinedJournal) {
        override def randomTimeout: Long = 1234L
      }, PaxosAgent(0, Recoverer, initialData, initialQuorumStrategy), vote)
      // then
      role match {
        case Recoverer => // good
        case x => fail(x.toString)
      }
      state match {
        case `initialData` => // good
        case x => fail(x.toString)
      }
    }
    "not broadcast messages and backs down if it gets a majority nack" in {
      // given
      val broadcastValues: ArrayBuffer[TimeAndMessage] = ArrayBuffer()
      val handler = new TestPrepareResponseHandlerNoRetransmission
      val vote = PrepareNack(recoverHighPrepare.id, 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), 12, 13)
      // when
      val PaxosAgent(_, role, _, _) = handler.handlePrepareResponse(new TestIO(new UndefinedJournal) {
        override def randomTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = {
          val update = TimeAndMessage(msg, System.nanoTime())
          broadcastValues += update
        }
      }, PaxosAgent(0, Recoverer, selfNackPrepares, initialQuorumStrategy), vote)
      // then we are a follower
      role match {
        case Follower => // good
        case x => fail(x.toString)
      }
      // and we sent no messages
      broadcastValues.size shouldBe 0
    }
    "issues another prepare and an accept if majority ack shows higher accepted index" in {
      // given
      val broadcastValues: ArrayBuffer[TimeAndMessage] = ArrayBuffer()
      val handler = new TestPrepareResponseHandlerNoRetransmission
      val otherAcceptedIndex = 2L // recoverHighPrepare.id.logIndex + 1
      val vote = PrepareAck(recoverHighPrepare.id, 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), otherAcceptedIndex, 13, None)
      // when
      val PaxosAgent(_, role, data, _) = handler.handlePrepareResponse(new TestIO(emptyJournal) {
        override def randomTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = {
          val update = TimeAndMessage(msg, System.nanoTime())
          broadcastValues += update
        }
      }, PaxosAgent(0, Recoverer, selfAckPrepares, initialQuorumStrategy), vote)
      // then we are still a recoverer as not finished with all prepares
      role match {
        case Recoverer => // good
        case x => fail(x.toString)
      }
      // and we send one prepare and one accept
      broadcastValues match {
        case ArrayBuffer(TimeAndMessage(p: Prepare, _), TimeAndMessage(a: Accept, _)) =>
          p match {
            case p if p.id.logIndex == otherAcceptedIndex => // good
            case f => fail(f.toString)
          }
          a match {
            case a if a.id.logIndex == 1L => // good
            case f => fail(f.toString)
          }
        case f => fail(f.toString)
      }
      // and we accepted our own new prepare
      data.prepareResponses.headOption.value match {
        case (id, map) if id.logIndex == 2 => map.get(0) match {
          case Some(r: PrepareAck) => // good
          case f => fail(f.toString)
        }
        case f => fail(f.toString)
      }
    }
    "issues another prepare and an accept if majority ack shows higher accepted index but self nacks if has given higher promise" in {
      // given
      val broadcastValues: ArrayBuffer[TimeAndMessage] = ArrayBuffer()
      val handler = new TestPrepareResponseHandlerNoRetransmission
      val otherAcceptedIndex = 2L // recoverHighPrepare.id.logIndex + 1
      val vote = PrepareAck(recoverHighPrepare.id, 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), otherAcceptedIndex, 13, None)
      val higherPromise = Progress.highestPromisedLens.set(selfAckPrepares.progress, BallotNumber(Int.MaxValue, Int.MaxValue))
      // when
      val PaxosAgent(_, role, data, _) = handler.handlePrepareResponse(new TestIO(new UndefinedJournal) {
        override def randomTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = {
          val update = TimeAndMessage(msg, System.nanoTime())
          broadcastValues += update
        }
      }, PaxosAgent(0, Recoverer, selfAckPrepares.copy(progress = higherPromise), initialQuorumStrategy), vote)
      // then we are still a recoverer as not finished with all prepares
      role match {
        case Recoverer => // good
        case x => fail(x.toString)
      }
      // and we send on prepare and one accept
      broadcastValues match {
        case ArrayBuffer(pAndTime: TimeAndMessage, aAndTime: TimeAndMessage) =>
          pAndTime.message match {
            case p: Prepare if p.id.logIndex == otherAcceptedIndex => // good
            case f => fail(f.toString)
          }
          aAndTime.message match {
            case a: Accept if a.id.logIndex == 1L => // good
            case f => fail(f.toString)
          }
        case f => fail(f.toString)
      }
      // and we did not accept our own new prepare
      data.prepareResponses.headOption.getOrElse(fail) match {
        case (id, map) if id.logIndex == 2 => map.get(0) match {
          case Some(r: PrepareNack) => // good
          case f => fail(f.toString)
        }
        case f => fail(f.toString)
      }
    }
    "issues an accept which it journals if it has not made a higher promise" in {
      // given a handler that records broadcast time
      val broadcastValues: ArrayBuffer[TimeAndMessage] = ArrayBuffer()
      val handler = new TestPrepareResponseHandlerNoRetransmission
      // and an ack vote showing no higher accepted log index
      val vote = PrepareAck(recoverHighPrepare.id, 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), recoverHighPrepare.id.logIndex, 13, None)
      // and a journal which records save and send time
      val saved: Box[TimeAndParameter] = new Box(None)
      val journal = new UndefinedJournal {
        override def accept(a: Accept*): Unit = saved(TimeAndParameter(System.nanoTime(), a))
      }
      // and an IO that records send time
      val io = new TestIO(journal) {
        override def randomTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = {
          val update = TimeAndMessage(msg, System.nanoTime())
          broadcastValues += update
        }
      }
      // when
      val PaxosAgent(_, role, data, _) = handler.handlePrepareResponse(io, PaxosAgent(0, Recoverer, selfAckPrepares, initialQuorumStrategy), vote)
      // then we promote to leader
      role match {
        case Leader => // good
        case x => fail(x.toString)
      }
      // and we send one accept
      broadcastValues match {
        case ArrayBuffer(TimeAndMessage(a: Accept, _)) =>
          a match {
            case a if a.id.logIndex == 1L => // good
            case f => fail(f.toString)
          }
        case f => fail(f.toString)
      }
      // and we have the self vote for the accept
      data.acceptResponses.headOption.getOrElse(fail) match {
        case (id, AcceptResponsesAndTimeout(_, a: Accept, responses)) if id == recoverHighPrepare.id && a.id == id =>
          responses.headOption.getOrElse(fail) match {
            case (0, r: AcceptAck) => // good
            case f => fail(f.toString)
          }
        case f => fail(f.toString)
      }
      // and we have journalled the accept at a time before sent
      val sendTime = broadcastValues.headOption.value match {
        case TimeAndMessage(a: Accept, ts: Long) => ts
        case f => fail(f.toString)
      }
      assert(saved().time < sendTime)
      saved().parameter match {
        case Seq(a: Accept) if a.id.logIndex == 1 => // good
        case f => fail(f.toString)
      }
    }
    "issues an accept which it does not journal if it has made a higher promise" in {
      // given a handler
      val broadcastValues: ArrayBuffer[TimeAndMessage] = ArrayBuffer()
      val handler = new TestPrepareResponseHandlerNoRetransmission
      // and an ack vote showing no higher accepted log index
      val vote = PrepareAck(recoverHighPrepare.id, 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), 11L)), recoverHighPrepare.id.logIndex, 13, None)
      // when
      val higherPromise = Progress.highestPromisedLens.set(selfAckPrepares.progress, BallotNumber(Int.MaxValue, Int.MaxValue))
      val PaxosAgent(_, role, data, _) = handler.handlePrepareResponse(new TestIO(new UndefinedJournal) {
        override def randomTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = {
          val update = TimeAndMessage(msg, System.nanoTime())
          broadcastValues += update
        }

      }, PaxosAgent(0, Recoverer, selfAckPrepares.copy(progress = higherPromise), initialQuorumStrategy), vote)
      // then we promote to leader
      role match {
        case Leader => // good
        case x => fail(x.toString)
      }
      // and we send one accept
      broadcastValues match {
        case ArrayBuffer(TimeAndMessage(a: Accept, _)) =>
          a match {
            case a if a.id.logIndex == 1L => // good
            case f => fail(f.toString)
          }
        case f => fail(f.toString)
      }
      // and we have the self vote for the accept
      data.acceptResponses.headOption.getOrElse(fail) match {
        case (id, AcceptResponsesAndTimeout(_, a: Accept, responses)) if id == recoverHighPrepare.id && a.id == id =>
          responses.headOption.getOrElse(fail) match {
            case (0, r: AcceptNack) => // good
            case f => fail(f.toString)
          }
        case f => fail(f.toString)
      }
      // and the undefined journal would have blown up had we tried to journal that accept that was below our promise.
    }
    "invokes request retransmission function when sees a committed slot higher than its own" in {
      // given
      val sentValues: ArrayBuffer[PaxosMessage] = ArrayBuffer()
      val handler = new PrepareResponseHandler with BackdownAgent
      val otherCommittedIndex = 11L
      val vote = PrepareAck(recoverHighPrepare.id, 5, Progress(BallotNumber(6, 7), Identifier(8, BallotNumber(9, 10), otherCommittedIndex)), 12L, 13, None)

      // when
      val PaxosAgent(_, role, data, _) = handler.handlePrepareResponse(new TestIO(emptyJournal) {
        override def randomTimeout: Long = 1234L

        override def send(msg: PaxosMessage): Unit = {
          sentValues += msg
        }
      }, PaxosAgent(0, Recoverer, selfAckPrepares, initialQuorumStrategy), vote)

      // then
      sentValues.headOption.value shouldBe (RetransmitRequest(0, 5, selfAckPrepares.progress.highestCommitted.logIndex))
    }
    "chooses the highest value upon a majority ack" in {

      // given a hander to test
      val handler = new TestPrepareResponseHandlerNoRetransmission

      // and some recognisable accepts to choose from

      val v1 = DummyCommandValue(1)
      val v2 = DummyCommandValue(2)
      val v3 = DummyCommandValue(3)

      val id1 = Identifier(1, BallotNumber(1, 1), 1L)
      val id2 = Identifier(2, BallotNumber(2, 2), 1L)
      val id3 = Identifier(3, BallotNumber(3, 3), 1L)

      val a1 = Accept(id1, v1)
      val a2 = Accept(id2, v2)
      val a3 = Accept(id3, v3)

      // and our own high prepare and high progress

      val recoverHighNumber = BallotNumber(Int.MaxValue, 0)
      val recoverHighPrepare = Prepare(Identifier(0, recoverHighNumber, 1L))
      val progress = Progress.highestPromisedLens.set(selfAckPrepares.progress, recoverHighNumber)

      // and two acks one of them with the highest accept

      val prepareResponses = SortedMap.empty[Identifier, Map[Int, PrepareResponse]] ++
        Seq(
          (recoverHighPrepare.id -> Map(
            1 -> PrepareAck(recoverHighPrepare.id, 1, initialData.progress, 0, 0, Some(a1)),
            3 -> PrepareAck(recoverHighPrepare.id, 3, initialData.progress, 0, 0, Some(a3))
          ))
        )

      // and a vote with a different accept
      val vote = PrepareAck(recoverHighPrepare.id, 2, initialData.progress, 0, 0, Some(a2))

      // and our agent ready to choose the highest accept
      val agent = PaxosAgent(0, Recoverer, initialData.copy(clusterSize = () => 5, epoch = Some(recoverHighNumber), prepareResponses = prepareResponses, progress = progress), initialQuorumStrategy)

      // and a capturing IO
      val buffer = ArrayBuffer[PaxosMessage]()
      val io = new TestIO(noopJournal) {
        override def send(msg: PaxosMessage): Unit = {
          buffer += msg
        }

      }

      // when we get the majority
      handler.handlePrepareResponse(io, agent, vote)

      // then we have chosen the highest value
      buffer.headOption.value shouldBe Accept(recoverHighPrepare.id, v3)
    }
  }
  "chooses a noop value if free to do so upon a majority ack" in {

    // given a hander to test
    val handler = new TestPrepareResponseHandlerNoRetransmission

    // and our own high prepare and high progress

    val recoverHighNumber = BallotNumber(Int.MaxValue, 0)
    val recoverHighPrepare = Prepare(Identifier(0, recoverHighNumber, 1L))
    val progress = Progress.highestPromisedLens.set(selfAckPrepares.progress, recoverHighNumber)

    // and two acks none having accepted values

    val prepareResponses = SortedMap.empty[Identifier, Map[Int, PrepareResponse]] ++
      Seq(
        (recoverHighPrepare.id -> Map(
          1 -> PrepareAck(recoverHighPrepare.id, 1, initialData.progress, 0, 0, None),
          3 -> PrepareAck(recoverHighPrepare.id, 3, initialData.progress, 0, 0, None)
        ))
      )

    // and a third vote also without an accepted value
    val vote = PrepareAck(recoverHighPrepare.id, 2, initialData.progress, 0, 0, None)

    // and our agent ready to choose the highest accept
    val agent = PaxosAgent(0, Recoverer, initialData.copy(clusterSize = () => 5, epoch = Some(recoverHighNumber), prepareResponses = prepareResponses, progress = progress), initialQuorumStrategy)

    // and a capturing IO
    val buffer = ArrayBuffer[PaxosMessage]()
    val io = new TestIO(noopJournal) {
      override def send(msg: PaxosMessage): Unit = {
        buffer += msg
      }
    }

    // when we get the majority
    handler.handlePrepareResponse(io, agent, vote)

    // then we have chosen the highest value
    buffer.headOption.value shouldBe Accept(recoverHighPrepare.id, NoOperationCommandValue)
  }
  "backsdown if we have an even number of nodes and a split vote" in {
    // given a handler that records when backdown has been called
    val handler = new TestPrepareResponseHandlerNoRetransmission

    // and our own high prepare and high progress

    val recoverHighNumber = BallotNumber(Int.MaxValue, 0)
    val recoverHighPrepare = Prepare(Identifier(0, recoverHighNumber, 1L))
    val progress = Progress.highestPromisedLens.set(selfAckPrepares.progress, recoverHighNumber)

    // and two acks and one nack

    val prepareResponses = SortedMap.empty[Identifier, Map[Int, PrepareResponse]] ++
      Seq(
        (recoverHighPrepare.id -> Map(
          1 -> PrepareAck(recoverHighPrepare.id, 1, initialData.progress, 0, 0, None),
          2 -> PrepareNack(recoverHighPrepare.id, 2, initialData.progress, 0, 0),
          3 -> PrepareAck(recoverHighPrepare.id, 3, initialData.progress, 0, 0, None)
        ))
      )

    // and the forth vote is a nack
    val vote = PrepareNack(recoverHighPrepare.id, 4, initialData.progress, 0, 0)

    // and our agent ready to choose back down on a split vote as cluster size is 4
    val agent = PaxosAgent(0, Recoverer, initialData.copy(clusterSize = () => 4, epoch = Some(recoverHighNumber), prepareResponses = prepareResponses, progress = progress), initialQuorumStrategy4)

    // and a do nothing IO
    val io = new TestIO(noopJournal) {
      override def logger: PaxosLogging = NoopPaxosLogging
    }

    // when we get the majority
    val PaxosAgent(_, role, newData, _) = handler.handlePrepareResponse(io, agent, vote)

    // then
    role shouldBe Follower
    newData.epoch shouldBe None
    newData.prepareResponses.isEmpty shouldBe true

  }
  "records a vote if it does not have a majority response" in {
    // given a handler to test
    val handler = new TestPrepareResponseHandlerNoRetransmission

    // and our own high prepare and high progress

    val recoverHighNumber = BallotNumber(Int.MaxValue, 0)
    val recoverHighPrepare = Prepare(Identifier(0, recoverHighNumber, 1L))
    val progress = Progress.highestPromisedLens.set(selfAckPrepares.progress, recoverHighNumber)

    // a self ack

    val prepareResponses = SortedMap.empty[Identifier, Map[Int, PrepareResponse]] ++
      Seq(
        (recoverHighPrepare.id -> Map(
          1 -> PrepareAck(recoverHighPrepare.id, 0, initialData.progress, 0, 0, None)
        ))
      )

    // and then a nack so no majority and no split vote
    val vote = PrepareNack(recoverHighPrepare.id, 2, initialData.progress, 0, 0)

    // and our agent ready to choose the highest accept
    val agent = PaxosAgent(0, Recoverer, initialData.copy(clusterSize = () => 5, epoch = Some(recoverHighNumber), prepareResponses = prepareResponses, progress = progress), initialQuorumStrategy)

    // when we get the majority
    val PaxosAgent(_, role, data, _) = handler.handlePrepareResponse(undefinedSilentIO, agent, vote)

    // then we have simply recorded the vote
    role shouldBe Recoverer
    data.prepareResponses.size shouldBe 1
    data.prepareResponses.headOption.value shouldBe (recoverHighPrepare.id -> Map(
      1 -> PrepareAck(recoverHighPrepare.id, 0, initialData.progress, 0, 0, None),
      2 -> PrepareNack(recoverHighPrepare.id, 2, initialData.progress, 0, 0)
    ))
  }

  "ignores a late response" in {
    // given a handler to test
    val recoverHighNumber = BallotNumber(Int.MaxValue, 0)
    val progress = Progress.highestPromisedLens.set(selfAckPrepares.progress, recoverHighNumber)

    // and an agent ready to choose the highest accept
    val agent: PaxosAgent =
      PaxosAgent(0, Recoverer, initialData.copy(clusterSize = () => 5, epoch = Some(recoverHighNumber),
        progress = progress), initialQuorumStrategy)

    val lenses: PaxosLenses = new TestPrepareResponseHandlerNoRetransmission

    // when we get the majority
    val prepareResponses = PrepareResponseHandler.expandedPrepareSlotRange(undefinedIO, lenses, agent, SortedMap.empty)

    // then we have simply recorded the vote
    prepareResponses.size shouldBe 0
  }
}
