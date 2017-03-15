package com.github.trex_paxos.core

import akka.actor.ActorRef
import _root_.com.github.trex_paxos.library._
import _root_.com.github.trex_paxos.PaxosProperties

import scala.collection.mutable.ArrayBuffer

class TestAkkaPaxosActorNoTimeout(config: PaxosProperties, clusterSizeF: () => Int, nodeUniqueId: Int, broadcastRef: ActorRef, journal: Journal, val delivered: ArrayBuffer[CommandValue], tracer: Option[AkkaPaxosActor.Tracer])
  extends AkkaPaxosActorNoTimeout(config, nodeUniqueId, journal) {

  // does nothing but makes this class concrete for testing
  val deliverClient: PartialFunction[Payload, Array[Byte]] = {
    case x => NoOperationCommandValue.bytes
  }

  override def deliver(payload: Payload): Array[Byte] = {
    delivered.append(payload.command)
    payload.command match {
      case ClientCommandValue(_, bytes) => if (bytes.length > 0) Array[Byte]((-bytes(0)).toByte) else bytes
      case noop@NoOperationCommandValue => noop.bytes
    }
  }

  def setAgent(role: PaxosRole, data: PaxosData) = this.paxosAgent = this.paxosAgent.copy(role = role, data = data)

  def role = this.paxosAgent.role

  def data = this.paxosAgent.data

  override def clusterSize: Int = clusterSizeF()

  override def broadcast(msg: PaxosMessage): Unit = send(broadcastRef, msg)
}