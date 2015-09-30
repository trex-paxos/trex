package com.github.simbo1905.trex.internals

import akka.actor.ActorRef
import akka.testkit.{TestActorRef, TestKit}
import com.github.simbo1905.trex.library._
import org.scalamock.scalatest.MockFactory

import scala.collection.mutable.ArrayBuffer

trait NotLeaderSpec { self: TestKit with MockFactory with AllStateSpec =>
  import AllStateSpec._
  import PaxosActor.Configuration

  def respondsToClientDataBySayingNotTheLeader(state: PaxosRole)(implicit sender: ActorRef) {
    require(state == Follower || state == Recoverer)
    val stubJournal: Journal = stub[Journal]
    // given a node in the prescribed state
    val fsm = TestActorRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, stubJournal, ArrayBuffer.empty, None))
    fsm.underlyingActor.setAgent(state, initialData)
    // when it gets arbitrary client data
    val value = ClientRequestCommandValue(0, "hello world".getBytes("UTF8"))
    fsm ! value
    // it responds that it is not the leader
    expectMsg(NotLeader(0, 0))

  }
}