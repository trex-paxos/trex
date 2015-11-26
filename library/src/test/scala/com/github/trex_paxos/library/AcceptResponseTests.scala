package com.github.trex_paxos.library

import java.util.concurrent.atomic.AtomicLong

import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OptionValues, WordSpecLike}

import scala.collection.mutable.ArrayBuffer

class AcceptResponseTests extends WordSpecLike with Matchers with MockFactory with OptionValues {

  import TestHelpers._

  "AcceptResponsesHandler" should {

    "ignores a response it is not awaiting" in {
      // given
      val handler = new Object with AcceptResponseHandler
      val agent = PaxosAgent(0, Leader, initialData97.copy(acceptResponses = acceptSelfAck98))

      // when
      val PaxosAgent(_, role, data) = handler.handleAcceptResponse(undefinedSilentIO, agent, a99ack1)

      // then
      role shouldBe agent.role
      data.acceptResponses.size shouldBe 1
      data.acceptResponses.headOption match {
        case Some((id, AcceptResponsesAndTimeout(_, a, map))) if id == a98.id =>
          a shouldBe a98
          map.size shouldBe 1
          map.getOrElse(0, fail()) shouldBe a98ack0
        case f => fail(f.toString)
      }
    }

    "ignores a repeated response it is not awaiting" in {
      // given
      val handler = new Object with AcceptResponseHandler
      val agent = PaxosAgent(0, Leader, initialData97.copy(acceptResponses = acceptSelfAck98))

      // when
      val PaxosAgent(_, role, data) = handler.handleAcceptResponse(undefinedSilentIO, agent, a98ack0)

      // then
      role shouldBe agent.role
      data.acceptResponses.size shouldBe 1
      data.acceptResponses.headOption match {
        case Some((id, AcceptResponsesAndTimeout(_, a, map))) if id == a98.id =>
          a shouldBe a98
          map.size shouldBe 1
          map.getOrElse(0, fail()) shouldBe a98ack0
        case f => fail(f.toString)
      }
    }

    "records a vote it is looking for when not got a majority ack or nack" in {
      // given
      val handler = new Object with AcceptResponseHandler
      val agent = PaxosAgent(0, Leader, initialData97.copy(acceptResponses = acceptSelfAck98))
      val randomTimeoutIO = new UndefinedIO with SilentLogging {
        override def randomTimeout: Long = Long.MaxValue
      }

      // when
      val PaxosAgent(_, role, data) = handler.handleAcceptResponse(randomTimeoutIO, agent, a98nack1)

      // then
      role shouldBe agent.role
      data.acceptResponses.size shouldBe 1
      data.acceptResponses.headOption match {
        case Some((id, AcceptResponsesAndTimeout(_, a, map))) if id == a98.id =>
          a shouldBe a98
          map.size shouldBe 2
          map.getOrElse(0, fail()) shouldBe a98ack0
          map.getOrElse(1, fail()) shouldBe a98nack1
        case f => fail(f.toString)
      }
    }

    "backdown if sees higher log index in a commit message" in {
      // given
      val handler = new Object with AcceptResponseHandler
      val agent = PaxosAgent(0, Leader, initialData97.copy(acceptResponses = acceptSelfAck98))
      val ioRandomTimeout = new UndefinedIO with SilentLogging {
        override def randomTimeout: Long = Long.MaxValue
      }

      // when
      val PaxosAgent(_, role, data) = handler.handleAcceptResponse(ioRandomTimeout, agent, a98ackProgress98)

      // then
      role shouldBe Follower
      data.acceptResponses.size shouldBe 0
      data.timeout shouldBe Long.MaxValue
    }

    "backdown if sees a majority nack" in {
      // given
      val handler = new Object with AcceptResponseHandler
      val agent = PaxosAgent(0, Leader, initialData97.copy(acceptResponses = acceptSplitAckAndNack))
      val ioRandomTimeout = new UndefinedIO with SilentLogging {
        override def randomTimeout: Long = Long.MaxValue
      }

      // when
      val PaxosAgent(_, role, data) = handler.handleAcceptResponse(ioRandomTimeout, agent, a98nack2)

      // then
      role shouldBe Follower
      data.acceptResponses.size shouldBe 0
      data.timeout shouldBe Long.MaxValue
    }

    "records the vote if it got a majority ack out of sequence" in {
      // given
      val handler = new Object with AcceptResponseHandler
      val agent = PaxosAgent(0, Leader, initialData97.copy(acceptResponses = acceptSelfAck98and99))
      val ioRandomTimeout = new UndefinedIO with SilentLogging {
        override def randomTimeout: Long = Long.MaxValue
      }

      // when
      val PaxosAgent(_, role, data) = handler.handleAcceptResponse(ioRandomTimeout, agent, a99ack1)

      // then
      role shouldBe agent.role
      data.acceptResponses.size shouldBe 2
      data.acceptResponses(a98.id) match {
        case AcceptResponsesAndTimeout(_, accept, votes) =>
          accept shouldBe a98
          votes.size shouldBe 1
      }
      data.acceptResponses(a99.id) match {
        case AcceptResponsesAndTimeout(_, accept, votes) =>
          accept shouldBe a99
          votes.isEmpty shouldBe true
      }
    }

    "backdown if it sees a majority ack but committable slows are not contiguous with highest committed" in {
      // given
      val handler = new Object with AcceptResponseHandler
      val agent = PaxosAgent(0, Leader, initialData96.copy(acceptResponses = acceptSelfAck98))
      val ioRandomTimeout = new UndefinedIO with SilentLogging {
        override def randomTimeout: Long = Long.MaxValue
      }

      // when
      val PaxosAgent(_, role, data) = handler.handleAcceptResponse(ioRandomTimeout, agent, a98ack1)

      // then
      role shouldBe Follower
      data.acceptResponses.size shouldBe 0
    }

    "commit multiple contiguous slots on a majority ack" in {
      // given
      val handler = new Object with AcceptResponseHandler
      val agent = PaxosAgent(0, Leader, initialData97.copy(acceptResponses = acceptAck98and99empty))
      val sent: ArrayBuffer[PaxosMessage] = ArrayBuffer()
      val mockJournal = stub[Journal]
      mockJournal.accepted _ when (98L) returns Some(a98)
      mockJournal.accepted _ when (99L) returns Some(a99)
      val ioRandomTimeout = new UndefinedIO with SilentLogging {
        override def journal: Journal = mockJournal

        override def send(msg: PaxosMessage): Unit = sent += msg
      }

      // when
      val PaxosAgent(_, role, data) = handler.handleAcceptResponse(ioRandomTimeout, agent, a98ack1)

      // then
      role shouldBe Leader
      data.acceptResponses.size shouldBe 0
      data.progress.highestCommitted.logIndex shouldBe 99L
      sent.size shouldBe 1
      sent.headOption.value match {
        case Commit(id, _) if id == a99.id => // good
        case f => fail(f.toString)
      }
    }

    "saves before sending" in {
      // given data ready commit
      val numberOfNodes = 3
      val selfAcceptResponses = emptyAcceptResponses98 +
        (a98.id -> AcceptResponsesAndTimeout(50L, a98, Map(0 -> AcceptAck(a98.id, 0, progress97))))
      val data = initialData.copy(clusterSize = numberOfNodes,
        progress = progress97,
        epoch = Some(a98.id.number),
        acceptResponses = selfAcceptResponses)

      // when we send accept to the handler which records the send time and save time
      val sendTime = new AtomicLong
      val saveTime = new AtomicLong
      val vote = AcceptAck(a98.id, 1, progress97)
      val handler = new UndefinedAcceptResponseHandler {
        override def commit(io: PaxosIO, agent: PaxosAgent, identifier: Identifier): (Progress, Seq[(Identifier, Any)]) =
          (progress98, Seq.empty)
      }

      val testJournal = new UndefinedJournal {
        override def save(progress: Progress): Unit = saveTime.set(System.nanoTime())
      }
      val PaxosAgent(_, _, _) = handler.handleAcceptResponse(new TestIO(testJournal) {
        override def send(msg: PaxosMessage): Unit = sendTime.set(System.nanoTime())
      }, PaxosAgent(0, Recoverer, data), vote)
      // then we saved before we sent
      assert(saveTime.get > 0)
      assert(sendTime.get > 0)
      assert(saveTime.get < sendTime.get)
    }

    "responds to the clients who's command have been committed" in {
      // given
      val handler = new Object with AcceptResponseHandler
      val clientCommands: Map[Identifier, (CommandValue, String)] = Map(
        (a100.id ->(NoOperationCommandValue, DummyRemoteRef(100))),
        (a98.id ->(NoOperationCommandValue, DummyRemoteRef(98))),
        (a101.id ->(NoOperationCommandValue, DummyRemoteRef(101))),
        (a99.id ->(NoOperationCommandValue, DummyRemoteRef(99)))
      )
      val agent = PaxosAgent(0, Leader, initialData97.copy(acceptResponses = acceptAck98and99empty, clientCommands = clientCommands))
      val mockJournal = stub[Journal]
      mockJournal.accepted _ when (98L) returns Some(a98)
      mockJournal.accepted _ when (99L) returns Some(a99)
      val responds: ArrayBuffer[String] = ArrayBuffer()
      val ioRandomTimeout = new UndefinedIO with SilentLogging {
        override def journal: Journal = mockJournal

        override def send(msg: PaxosMessage): Unit = {}

        override def respond(client: String, data: Any): Unit = responds += client
      }

      // when
      val PaxosAgent(_, role, data) = handler.handleAcceptResponse(ioRandomTimeout, agent, a98ack1)

      // then
      role shouldBe Leader
      data.acceptResponses.size shouldBe 0
      data.progress.highestCommitted.logIndex shouldBe 99L
      responds.size shouldBe 2
      responds.contains(DummyRemoteRef(98)) shouldBe true
      responds.contains(DummyRemoteRef(99)) shouldBe true
    }

    "deals with a split vote in even number sized cluster" in {
      // given
      val handler = new Object with AcceptResponseHandler
      val agent = PaxosAgent(0, Leader, initialData97.copy(clusterSize = 4, acceptResponses = acceptkAndTwoNack98))
      val ioRandomTimeout = new UndefinedIO with SilentLogging {
        override def randomTimeout: Long = Long.MaxValue
      }

      // when
      val PaxosAgent(_, role, data) = handler.handleAcceptResponse(ioRandomTimeout, agent, a98ack3)

      // then
      role shouldBe Follower
      data.acceptResponses.size shouldBe 0
      data.timeout shouldBe Long.MaxValue
    }

    "logs an error if we have not issued accept messages for slots contiguous with highest committed" in {
      // given
      val handler = new Object with AcceptResponseHandler
      // an agent committed up to slot 96 awaiting responses only on slot 98 so got illegal gap at slot 97
      val (id, AcceptResponsesAndTimeout(_, accept, responses)) = acceptkAndTwoNack98.headOption.value
      val agent = PaxosAgent(0, Leader, initialData96.copy(clusterSize = 3, acceptResponses = acceptkAndTwoNack98))
      val errorLog = ArrayBuffer[String]()
      val ioRandomTimeout = new UndefinedIO {
        override def randomTimeout: Long = Long.MaxValue

        override def plog: PaxosLogging = new EmptyLogging {
          override def error(msg: String): Unit = errorLog += msg
        }
      }
      val latestVotes = responses + (a98ack3.from -> a98ack3)
      // when
      val PaxosAgent(_, role, data) = handler.handleFreshResponse(ioRandomTimeout, agent, latestVotes, accept, a98ack3)

      // then
      role shouldBe Follower
      data.acceptResponses.size shouldBe 0
      data.timeout shouldBe Long.MaxValue
      assert(errorLog.mkString("").contains("committable work which is not contiguous with progress implying we have not issued Prepare/Accept messages for the correct range of slots"))
    }
  }
}

class UndefinedAcceptResponseHandler extends AcceptResponseHandler {

  override def commit(io: PaxosIO, agent: PaxosAgent, identifier: Identifier): (Progress, Seq[(Identifier, Any)]) = throw new AssertionError("deliberately not implemented")

}