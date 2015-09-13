package com.github.simbo1905.trex.internals

import akka.actor.{ActorSystem, ActorRef}
import akka.testkit.{TestProbe, TestKit}
import com.github.simbo1905.trex.internals.AllStateSpec._
import com.github.simbo1905.trex.library._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpecLike}

import Ordering._

import scala.collection.immutable.{TreeMap, SortedMap}

object AcceptResponsesSpec {
  val identifier98: Identifier = Identifier(1, BallotNumber(1, 1), 98L)

  val a98 = Accept(identifier98, NoOperationCommandValue)

  val emptyAcceptResponses98: SortedMap[Identifier, AcceptResponsesAndTimeout] = TreeMap(
    a98.id -> AcceptResponsesAndTimeout(100L, a98, Map.empty)
  )

  val progress97 = Progress(BallotNumber(0, 0), Identifier(0, BallotNumber(0, 0), 97L))

  val progress98 = Progress(BallotNumber(0, 0), Identifier(0, BallotNumber(0, 0), 98L))
}

class AcceptResponsesSpec extends TestKit(ActorSystem("AcceptResponsesSpec", AllStateSpec.config))
with WordSpecLike with Matchers with MockFactory {

  import AcceptResponsesSpec._

  "AcceptResponsesHandler" should {
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
      var sendTime = 0L
      var saveTime = 0L
      val vote = AcceptAck(a98.id, 1, progress97)
      val handler = new UndefinedAcceptResponsesHandler {
        override def plog: PaxosLogging = NoopPaxosLogging

        override def broadcast(msg: Any) {
          sendTime = System.nanoTime()
        }

        override def commit(state: PaxosRole, data: PaxosData[ActorRef], identifier: Identifier, progress: Progress): (Progress, Seq[(Identifier, Any)]) =
          (progress98,Seq.empty)

        override def journalProgress(progress: Progress): Progress = {
          saveTime = System.nanoTime()
          progress98
        }
      }
      handler.handleAcceptResponse(0, Recoverer, TestProbe().ref, vote, data)
      // then we saved before we sent
      assert(saveTime > 0)
      assert(sendTime > 0)
      assert(saveTime < sendTime)
    }
  }
}

class UndefinedAcceptResponsesHandler extends AcceptResponsesHandler[ActorRef] {

  override def backdownData(data: PaxosData[ActorRef]): PaxosData[ActorRef] = ???

  override def sendNoLongerLeader(clientCommands: Map[Identifier, (CommandValue, ActorRef)]): Unit = {}

  override def randomTimeout: Long = 12345L

  override def send(actor: ActorRef, msg: Any): Unit = {}

  override def broadcast(msg: Any): Unit = {}

  override def commit(state: PaxosRole, data: PaxosData[ActorRef], identifier: Identifier, progress: Progress): (Progress, Seq[(Identifier, Any)]) = ???

  def journalProgress(progress: Progress): Progress = ???

  override def plog: PaxosLogging = NoopPaxosLogging
}