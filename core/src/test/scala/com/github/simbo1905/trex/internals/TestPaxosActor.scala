package com.github.simbo1905.trex.internals

import akka.actor.ActorRef
import com.github.simbo1905.trex._
import com.github.simbo1905.trex.internals.PaxosActor.TraceData

import scala.collection.mutable.ArrayBuffer

class TestPaxosActor(config: PaxosActor.Configuration, nodeUniqueId: Int, broadcast: ActorRef, journal: Journal, val delivered: ArrayBuffer[CommandValue], tracer: Option[PaxosActor.Tracer])
  extends PaxosActor(config, nodeUniqueId, broadcast, journal) {

  // does nothing but makes this class concrete for testing
  val deliverClient: PartialFunction[CommandValue, Array[Byte]] = {
    case m => NoOperationCommandValue.bytes
  }

  override def deliver(value: CommandValue): Array[Byte] = {
    delivered.append(value)
    value match {
      case ClientRequestCommandValue(_, bytes) => if (bytes.length > 0) Array[Byte]((-bytes(0)).toByte) else bytes
      case noop@NoOperationCommandValue => noop.bytes
    }
  }

  override def trace(state: PaxosRole, data: PaxosData, sender: ActorRef, msg: Any): Unit = {
    tracer.foreach(t => t(TraceData(nodeUniqueId, state, data, Some(sender), msg)))
  }

  override def trace(state: PaxosRole, data: PaxosData, payload: CommandValue): Unit = {
    tracer.foreach(t => t(TraceData(nodeUniqueId, state, data, None, payload)))
  }
}