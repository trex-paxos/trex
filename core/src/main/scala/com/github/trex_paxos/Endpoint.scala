package com.github.trex_paxos

import akka.actor.{ActorRef, TypedActor}
import akka.actor.TypedActor.MethodCall
import akka.serialization.SerializationExtension
import com.github.trex_paxos.internals.{PaxosActor, PaxosActorWithTimeout}
import com.github.trex_paxos.library.{CommandValue, Journal, Payload, ServerResponse}

import scala.util.Try

// TODO this should be a MethodCall mix-in rather than a concrete class
class TypedActorPaxosEndpoint(config: PaxosActor.Configuration, clusterSize: () => Int,
                              broadcastReference: ActorRef, nodeUniqueId: Int,
                              journal: Journal, target: AnyRef)
  extends PaxosActorWithTimeout(config, clusterSize, nodeUniqueId, broadcastReference, journal) {

  def broadcast(msg: Any): Unit = send(broadcastReference, msg)

  val serialization = SerializationExtension(context.system)
  val serializer = serialization.serializerFor(classOf[MethodCall])

  def deserialize(bytes: Array[Byte]): MethodCall = {
    serializer.fromBinary(bytes, manifest = None).asInstanceOf[MethodCall]
  }

  override val deliverClient: PartialFunction[Payload, AnyRef] = {
    case Payload(logIndex, c: CommandValue) =>
      val mc@TypedActor.MethodCall(method, parameters) = deserialize(c.bytes)
      log.debug("delivering slot {} value {}", logIndex, mc)
      val result = Try {
        val response = Option(method.invoke(target, parameters: _*))
        log.debug(s"invoked ${method.getName} returned $response")
        ServerResponse(c.msgId, response)
      } recover {
        case ex =>
          log.error(ex, s"call to $method with $parameters got exception $ex")
          ServerResponse(c.msgId, Option(ex))
      }
      result.get
    case f => throw new AssertionError("unreachable code")
  }
}
