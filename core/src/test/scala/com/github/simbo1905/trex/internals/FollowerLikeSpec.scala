package com.github.simbo1905.trex.internals

import akka.actor.ActorRef
import akka.testkit.{TestFSMRef, TestKit}
import org.scalamock.scalatest.MockFactory

import scala.collection.mutable.ArrayBuffer

trait FollowerLikeSpec { self: TestKit with MockFactory with AllStateSpec =>
  import AllStateSpec._
  import PaxosActor.Configuration

  def respondsToClientDataBySayingNotTheLeader(state: PaxosRole)(implicit sender: ActorRef) {
    require(state == Follower || state == Recoverer)
    // given a node in the prescribed state
    val fsm = TestFSMRef(new TestPaxosActor(Configuration(config, clusterSize3), 0, sender, stubJournal, ArrayBuffer.empty, None))
    fsm.setState(state, initialData)
    // when it gets arbitrary client data
    val value = ClientRequestCommandValue(0, "hello world".getBytes("UTF8"))
    fsm ! value
    // it responds that it is not the leader
    expectMsg(NotLeader(0, 0))

  }
}