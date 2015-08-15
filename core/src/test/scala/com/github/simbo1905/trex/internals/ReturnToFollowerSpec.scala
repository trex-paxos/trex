package com.github.simbo1905.trex.internals

import akka.actor.{ActorSystem, ActorRef}
import akka.event.LoggingAdapter
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.{Matchers, WordSpecLike}

class UndefinedReturnToFollowerHandler extends ReturnToFollowerHandler {
  val log: LoggingAdapter = NoopLoggingAdapter

  def randomTimeout: Long = 1234L

  def nodeUniqueId: Int = 0

  def commit(state: PaxosRole, data: PaxosData, identifier: Identifier, progress: Progress): (Progress, Seq[(Identifier, Any)]) = (progress, Seq.empty)

  def sendNoLongerLeader(clientCommands: Map[Identifier, (CommandValue, ActorRef)]): Unit = {}

  def send(actor: ActorRef, msg: Any): Unit = actor ! msg

}

class ReturnToFollowerSpec extends TestKit(ActorSystem("ReturnToFollowerSpec")) with WordSpecLike with Matchers {

  "ReturnToFollowerHandler message handling" should {
    "send retransmission if higher committed log index is seen" in {
      // give a handler
      val handler = new UndefinedReturnToFollowerHandler {

      }
      // and a commit message id higher than the intial data value of 0L
      val id = AllStateSpec.initialData.progress.highestCommitted.copy(logIndex = 99L, from = 2)
      // and a test sender
      val probe = TestProbe()
      // when we handle that message
      handler.handleReturnToFollowerOnHigherCommit(Commit(id), AllStateSpec.initialData, Recoverer, probe.ref)
      // then
      probe.expectMsg(RetransmitRequest(from = 0, to = 2, AllStateSpec.initialData.progress.highestCommitted.logIndex))
    }
  }
}
