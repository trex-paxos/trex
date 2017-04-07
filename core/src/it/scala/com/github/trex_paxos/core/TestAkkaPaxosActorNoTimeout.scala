package com.github.trex_paxos.core

import akka.actor.ActorRef
import _root_.com.github.trex_paxos.library._
import _root_.com.github.trex_paxos.PaxosProperties

import scala.collection.mutable.ArrayBuffer

// TODO can this be rationalised with TestAkkaPaxosActor
class TestAkkaPaxosActorNoTimeout(config: PaxosProperties, clusterSizeF: () => Int, nodeUniqueId: Int, broadcastRef: ActorRef, journal: Journal, val delivered: ArrayBuffer[CommandValue], tracer: Option[AkkaPaxosActor.Tracer])
  extends AkkaPaxosActorNoTimeout(config, nodeUniqueId, journal) {

  override def deliverMembership(payload: Payload): Array[Byte] = ???

  // echos the input back so that its easy to verify responses seen during testing
  override def deliverClient(payload: Payload): Array[Byte] = payload match {
    case Payload(_, c@ClientCommandValue(_, bytes)) =>
      log.debug(s"Node ${nodeUniqueId} delivered ${bytes}")
      delivered.append(c)
      if (bytes.length > 0) Array[Byte]((-bytes(0)).toByte) else bytes
    case Payload(_, noop@NoOperationCommandValue) =>
      delivered.append(noop)
      log.debug(s"Node ${nodeUniqueId} delivered NoOperationCommandValue")
      noop.bytes
  }

  def setAgent(role: PaxosRole, data: PaxosData) = this.paxosAgent = this.paxosAgent.copy(role = role, data = data)

  def role = this.paxosAgent.role

  def data = this.paxosAgent.data

  override def clusterSize: Int = clusterSizeF()

  override def broadcast(msg: PaxosMessage): Unit = send(broadcastRef, msg)
}