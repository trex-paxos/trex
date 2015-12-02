package com.github.trex_paxos.internals

import akka.actor.ActorRef
import PaxosActor.TraceData
import com.github.trex_paxos.library._

import scala.collection.mutable.ArrayBuffer

class TestPaxosActor(config: PaxosActor.Configuration, nodeUniqueId: Int, broadcastRef: ActorRef, journal: Journal, val delivered: ArrayBuffer[CommandValue], tracer: Option[PaxosActor.Tracer])
  extends PaxosActor(config, nodeUniqueId, broadcastRef, journal) {

  /**
   * Helper to initialize actor responses map. Converts actor refs to string and store the lookup for string to actor ref in the internal map
   * @param data Actor version that algorithm cannot use
   * @return Algorithm version with client refs as string
   */
  def setClientData(data: Map[Identifier, (CommandValue, ActorRef)]) = {
    data map { case (id, (cmd, ref)) =>
      val pathAsString = ref.path.toString
      actorRefWeakMap.put(pathAsString, ref)
      (id, (cmd, pathAsString))
    }
  }

  // for the algorithm to have no dependency on akka we need to assign a String IDs
  override def senderId: String = super.senderId

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

  def setAgent(role: PaxosRole, data: PaxosData) = this.paxosAgent = this.paxosAgent.copy(role = role, data = data)

  def role = this.paxosAgent.role

  def data = this.paxosAgent.data
}