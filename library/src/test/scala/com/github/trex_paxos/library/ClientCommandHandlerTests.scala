package com.github.trex_paxos.library

import org.scalatest.{OptionValues, Matchers, WordSpecLike}

import scala.collection.mutable.ArrayBuffer

class ClientCommandHandlerTests extends WordSpecLike
with Matchers
with OptionValues
with PaxosLenses {

  import TestHelpers._

  "ClientCommandHandler" should {
    "assign a command to the next slot" in {
      // given
      val agent = PaxosAgent(5, Leader, initialData.copy(epoch = Option(initialData.progress.highestPromised)))
      val value = DummyCommandValue(1)
      // when
      val acceptOpt = ClientCommandHandler.acceptFor(agent, value)
      // then
      acceptOpt match {
        case Accept(id, v) =>
          v shouldBe value
          id shouldBe Identifier(agent.nodeUniqueId,
            agent.data.epoch.value,
            agent.data.progress.highestCommitted.logIndex + 1)
        case f => fail(f.toString)
      }
    }
    "ack if has not made a higher promise" in {
      // given a leader
      val agent: PaxosAgent = PaxosAgent(5, Leader, initialData.copy(epoch = Option(initialData.progress.highestPromised)))
      // and a fresh timeout
      val ioWithTimeout = new UndefinedIO {
        override def randomTimeout: Long = 12345L
      }
      // and a high accept
      val highAccept = Accept(Identifier(0, BallotNumber(Int.MaxValue, Int.MaxValue), Long.MaxValue), NoOperationCommandValue)
      // when
      val SelfAckOrNack(response, acceptResponses) = ClientCommandHandler.leaderSelfAckOrNack(ioWithTimeout, agent, highAccept)
      // then
      response match {
        case AcceptAck(id, node, progress)
          if id == highAccept.id && node == agent.nodeUniqueId && progress == agent.data.progress => // good
        case f => fail(f.toString)
      }
      acceptResponses.size shouldBe 1
      acceptResponses.get(highAccept.id) match {
        case Some(AcceptResponsesAndTimeout(12345L, highAccept, map)) =>
          map.get(agent.nodeUniqueId) match {
            case Some(AcceptAck(id, node, progress))
              if id == highAccept.id && node == agent.nodeUniqueId && progress == agent.data.progress => // good
            case f => fail(f.toString)
          }
        case f => fail(f.toString)
      }
    }
    "nacks if has made a higher promise" in {
      // given an Leader agent with a high promise
      val highPromiseData = highestPromisedLens.set(initialData, BallotNumber(Int.MaxValue, Int.MaxValue))
      val agent: PaxosAgent = PaxosAgent(5, Leader, highPromiseData.copy(epoch = Option(highPromiseData.progress.highestPromised)))
      // and a fresh timeout
      val ioWithTimeout = new UndefinedIO {
        override def randomTimeout: Long = 12345L
      }
      // and a low accept
      val highAccept = Accept(Identifier(0, BallotNumber(Int.MinValue, Int.MinValue), Long.MaxValue), NoOperationCommandValue)
      // when
      val SelfAckOrNack(response, acceptResponses) = ClientCommandHandler.leaderSelfAckOrNack(ioWithTimeout, agent, highAccept)
      // then
      response match {
        case AcceptNack(id, node, progress)
          if id == highAccept.id && node == agent.nodeUniqueId && progress == agent.data.progress => // good
        case f => fail(f.toString)
      }
      acceptResponses.size shouldBe 1
      acceptResponses.get(highAccept.id) match {
        case Some(AcceptResponsesAndTimeout(12345L, highAccept, map)) =>
          map.get(agent.nodeUniqueId) match {
            case Some(AcceptNack(id, node, progress))
              if id == highAccept.id && node == agent.nodeUniqueId && progress == agent.data.progress => // good
            case f => fail(f.toString)
          }
        case f => fail(f.toString)
      }
    }
    "does not journal a nack" in {
      // given a handler
      val handler = new Object with ClientCommandHandler
      // and a Leader agent with a high promise
      val highPromiseData = highestPromisedLens.set(initialData, BallotNumber(Int.MaxValue, Int.MaxValue))
      val agent: PaxosAgent =
        PaxosAgent(5, Leader, highPromiseData.copy(epoch = Option(BallotNumber(Int.MinValue, Int.MinValue))))
      // and a fresh timeout
      val ioWithTimeout = new UndefinedIO {
        override def randomTimeout: Long = 12345L

        override def send(msg: PaxosMessage): Unit = {}
      }
      // when
      val PaxosAgent(_, _, data) = handler.handleClientCommand(ioWithTimeout, agent, NoOperationCommandValue, DummyRemoteRef())
      // then
      data.acceptResponses.size shouldBe 1
      data.acceptResponses.headOption match {
        case Some((id, AcceptResponsesAndTimeout(12345L, highAccept, map))) =>
          map.get(agent.nodeUniqueId) match {
            case Some(AcceptNack(id, node, progress))
              if id == highAccept.id && node == agent.nodeUniqueId && progress == agent.data.progress => // good
            case f => fail(f.toString)
          }
        case f => fail(f.toString)
      }
    }
    "journals before sending" in {
      // given a handler
      val handler = new Object with ClientCommandHandler
      // and a leader
      val agent: PaxosAgent = PaxosAgent(5, Leader, initialData.copy(epoch = Option(initialData.progress.highestPromised)))
      // and an IO which records when it sent and saved
      val acceptedTs = ArrayBuffer[Long]()
      val sent = ArrayBuffer[Long]()
      val ioWithTimeout = new UndefinedIO {
        override def randomTimeout: Long = 12345L

        override def send(msg: PaxosMessage): Unit = sent += System.nanoTime()

        override def journal: Journal = new UndefinedJournal {

          override def accept(a: Accept*): Unit = acceptedTs += System.nanoTime()
        }
      }
      // when
      val PaxosAgent(_, _, data) = handler.handleClientCommand(ioWithTimeout, agent, NoOperationCommandValue, DummyRemoteRef())
      // then
      assert( acceptedTs.max < sent.min )
    }
    "holds onto the client ref" in {
      require(initialData.clientCommands.isEmpty)
      // given a handler
      val handler = new Object with ClientCommandHandler
      // and a leader
      val agent: PaxosAgent = PaxosAgent(5, Leader, initialData.copy(epoch = Option(initialData.progress.highestPromised)))
      // and a minimal IO that captures the sent accept
      val sent = ArrayBuffer[PaxosMessage]()
      val ioWithTimeout = new UndefinedIO {
        override def randomTimeout: Long = 12345L

        override def send(msg: PaxosMessage): Unit = sent += msg

        override def journal: Journal = new UndefinedJournal {

          override def accept(a: Accept*): Unit = {}
        }
      }
      // and a client
      val client = DummyRemoteRef()
      // and a dummy value
      val value = DummyCommandValue(1)
      // when
      val PaxosAgent(_, _, data) = handler.handleClientCommand(ioWithTimeout, agent, value, client)
      // then
      assert(data.clientCommands.nonEmpty)
      // and the sent accept is mapped to the client
      sent.headOption.value match {
        case accept: Accept =>
        assert(data.clientCommands.headOption.value == (accept.id, (value, client)))
        case f => fail(f.toString)
      }
    }
  }

}
