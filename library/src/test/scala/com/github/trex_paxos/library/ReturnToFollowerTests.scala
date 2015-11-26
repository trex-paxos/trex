package com.github.trex_paxos.library

import java.util.concurrent.atomic.AtomicReference

import org.scalatest.{Matchers, OptionValues, WordSpecLike}

import scala.language.postfixOps

class TestReturnToFollowerHandler extends ReturnToFollowerHandler {
  def commit(io: PaxosIO, agent: PaxosAgent, identifier: Identifier): (Progress, Seq[(Identifier, Any)]) = (agent.data.progress, Seq.empty)
}

class ReturnToFollowerTests extends WordSpecLike with Matchers with OptionValues {

  import TestHelpers._

  "ReturnToFollowerHandler message handling" should {
    "send retransmission if higher committed log index is seen" in {
      // give a handler
      val handler = new TestReturnToFollowerHandler
      // and a commit message id higher than the initial data value of 0L
      val id = initialData.progress.highestCommitted.copy(logIndex = 99L, from = 2)
      // when we handle that message
      val optMsg = new Box[PaxosMessage](None)
      handler.handleReturnToFollowerOnHigherCommit(new TestIO(new UndefinedJournal){
        override def send(msg: PaxosMessage): Unit = optMsg(msg)
      }, PaxosAgent(0, Recoverer, initialData), Commit(id))
      // then
      optMsg() shouldBe RetransmitRequest(from = 0, to = 2, initialData.progress.highestCommitted.logIndex)
    }

    "send no longer leader to any clients" in {
      // given a handler that collects client commands
      val clientCommands = collection.mutable.Map[Identifier, (CommandValue, String)]()
      val handler = new TestReturnToFollowerHandler
      // and a commit message id higher than the initial data value of 0L
      val id = initialData.progress.highestCommitted.copy(logIndex = 99L, from = 2)
      // and a some client commands
      val dataWithClient = initialData.copy(clientCommands = initialDataClientCommand)
      // when we handle that message
      handler.handleReturnToFollowerOnHigherCommit(new TestIO(new UndefinedJournal){
        override def sendNoLongerLeader(cc: Map[Identifier, (CommandValue, String)]): Unit = {
          clientCommands ++= cc
        }
      }, PaxosAgent(0, Recoverer, dataWithClient), Commit(id))
      // then the client is sent a NoLongerLeaderException
      clientCommands shouldBe dataWithClient.clientCommands
    }
  }
}
