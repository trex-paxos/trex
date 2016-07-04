package com.github.trex_paxos.internals

import akka.actor.ActorRef
import com.github.trex_paxos.library._
import com.github.trex_paxos.onakka.{PaxosActor, PaxosActorNoTimeout, PaxosProperties}

import scala.collection.mutable.ArrayBuffer

class TestPaxosActorNoTimeout(config: PaxosProperties, clusterSizeF: () => Int, nodeUniqueId: Int, broadcastRef: ActorRef, journal: Journal, val delivered: ArrayBuffer[CommandValue], tracer: Option[PaxosActor.Tracer])
  extends PaxosActorNoTimeout(config, nodeUniqueId, journal) {

  /**
   * Helper to initialize actor responses map. Converts actor refs to string and store the lookup for string to actor ref in the internal map
 *
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
  val deliverClient: PartialFunction[Payload, Array[Byte]] = {
    case x => NoOperationCommandValue.bytes
  }

  override def deliver(payload: Payload): Array[Byte] = {
    delivered.append(payload.command)
    payload.command match {
      case ClientRequestCommandValue(_, bytes) => if (bytes.length > 0) Array[Byte]((-bytes(0)).toByte) else bytes
      case noop@NoOperationCommandValue => noop.bytes
    }
  }

  def setAgent(role: PaxosRole, data: PaxosData) = this.paxosAgent = this.paxosAgent.copy(role = role, data = data)

  def role = this.paxosAgent.role

  def data = this.paxosAgent.data

  override def clusterSize: Int = clusterSizeF()

  override def broadcast(msg: PaxosMessage): Unit = send(broadcastRef, msg)
}