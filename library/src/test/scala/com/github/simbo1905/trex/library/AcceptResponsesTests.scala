package com.github.simbo1905.trex.library

import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpecLike}

class AcceptResponsesTests extends WordSpecLike with Matchers with MockFactory {

  import TestHelpers._

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
        override def commit(io: PaxosIO[DummyRemoteRef], agent: PaxosAgent[DummyRemoteRef], identifier: Identifier): (Progress, Seq[(Identifier, Any)]) =
          (progress98,Seq.empty)
      }

      val testJournal = new UndefinedJournal {
        override def save(progress: Progress): Unit = saveTime = System.nanoTime()
      }
      val PaxosAgent(_,_,_) = handler.handleAcceptResponse(new TestIO(testJournal){

        override def send(msg: PaxosMessage): Unit = sendTime = System.nanoTime()
      }, PaxosAgent(0, Recoverer, data), vote)
      // then we saved before we sent
      assert(saveTime > 0)
      assert(sendTime > 0)
      assert(saveTime < sendTime)
    }
  }
}

class UndefinedAcceptResponsesHandler extends AcceptResponsesHandler[DummyRemoteRef] {

  override def commit(io: PaxosIO[DummyRemoteRef], agent: PaxosAgent[DummyRemoteRef], identifier: Identifier): (Progress, Seq[(Identifier, Any)]) = throw new AssertionError("deliberately not implemented")

}