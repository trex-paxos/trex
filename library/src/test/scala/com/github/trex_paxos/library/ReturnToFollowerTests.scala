package com.github.trex_paxos.library

import scala.language.postfixOps

import org.scalatest._
import matchers.should._

class TestReturnToFollowerHandler extends ReturnToFollowerHandler {
  def commit(io: PaxosIO, agent: PaxosAgent, identifier: Identifier): (Progress, Seq[(Identifier, Any)]) = (agent.data.progress, Seq.empty)
}

class ReturnToFollowerTests extends wordspec.AnyWordSpec with Matchers with OptionValues {

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

        override def respond(results: Option[Map[Identifier, Any]]): Unit = {}
      }, PaxosAgent(0, Recoverer, initialData, initialQuorumStrategy), Commit(id))
      // then
      optMsg() shouldBe RetransmitRequest(from = 0, to = 2, initialData.progress.highestCommitted.logIndex)
    }

    "signal no response for clients" in {
      // given a flag to check that we signalled to clients that we are longer leader
      val noLongerLeader = Box(false)
      val handler = new TestReturnToFollowerHandler
      // and a commit message id higher than the initial data value of 0L
      val id = initialData.progress.highestCommitted.copy(logIndex = 99L, from = 2)
      // when we handle that message
      handler.handleReturnToFollowerOnHigherCommit(new TestIO(new UndefinedJournal){
        override def respond(results: Option[Map[Identifier, Any]]): Unit = results match {
          case None => noLongerLeader(true)
          case f => fail(f.toString)
        }
      }, PaxosAgent(0, Recoverer, initialData, initialQuorumStrategy), Commit(id))
      // then we signaled no responses
      noLongerLeader() shouldBe true
    }
  }
}
