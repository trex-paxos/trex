package com.github.trex_paxos

import java.net.InetSocketAddress

import akka.actor.{ActorRef, Props, TypedActor}
import akka.actor.TypedActor.MethodCall
import akka.serialization.SerializationExtension
import com.github.trex_paxos.internals._
import com.github.trex_paxos.library._

import scala.util.Try

// TODO should this be a MethodCall mix-in rather than a concrete class?
class TypedActorPaxosEndpoint(config: PaxosProperties, clusterSizeF: () => Int,
                              broadcastReference: ActorRef, nodeUniqueId: Int,
                              journal: Journal, target: AnyRef)
  extends PaxosActor(config, nodeUniqueId, journal) {

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

  override def clusterSize: Int = clusterSizeF()

  def broadcast(msg: PaxosMessage): Unit = send(broadcastReference, msg)
}

// TODO should this be a MethodCall mix-in rather than a concrete class
class TypedActorPaxosEndpoint2(
                                config: PaxosProperties,
                                selfNode: Node,
                                membershipStore: TrexMembership,
                                journal: Journal,
                                target: AnyRef)
  extends PaxosActor(config, selfNode.nodeUniqueId, journal) {

  val listenerRef = context.system.actorOf(Props(classOf[UdpListener],
    new InetSocketAddress(selfNode.host, selfNode.nodePort), self), s"UdpListener${selfNode.nodeUniqueId}")

  val serialization = SerializationExtension(context.system)
  val serializer = serialization.serializerFor(classOf[MethodCall])

  def deserialize(bytes: Array[Byte]): MethodCall = {
    serializer.fromBinary(bytes, manifest = None).asInstanceOf[MethodCall]
  }

  override val deliverClient: PartialFunction[Payload, AnyRef] = {
    case Payload(logIndex, c: ClientRequestCommandValue) =>
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
  }

  def senders(membership: Membership): Map[Int, ActorRef] = {
    val others = membership.members.filterNot(_.nodeUniqueId == nodeUniqueId)
    log.info("{} creating senders for nodes {}", nodeUniqueId, others)
    others.map { n =>
      import Member.pattern
      val pattern(host, port) = n.location
      n.nodeUniqueId -> context.system.actorOf(Props(classOf[UdpSender],
        new java.net.InetSocketAddress(host, port.toInt)), s"UdpSender${n.nodeUniqueId}")
    }.toMap
  }

  var others: Map[Int, ActorRef] = senders(membershipStore.loadMembership().getOrElse(Membership(Seq())))

  log.info(s"cluster members are ${others}")

  override val deliverMembership: PartialFunction[Payload, AnyRef] = {
    case Payload(logIndex, m: MembershipCommandValue) =>
      val result = Try {
        val membership = Membership(m.members)
        others = senders(membership)
        membershipStore.saveMembership(logIndex, membership)
        log.info(s"membership at logIndex ${logIndex} is $membership")
        ServerResponse(m.msgId, None)
      } recover {
        case ex =>
          log.error(ex, s"call save membership at logIndex ${logIndex} with membership ${m.members} got exception $ex")
          ServerResponse(m.msgId, Option(ex))
      }
      result.get
  }

  override def clusterSize: Int = others.size + 1

  override def broadcast(msg: PaxosMessage): Unit = others.values foreach { a =>
    log.debug("broadcast routing to {} msg {}", a, msg)
    a  ! msg
  }

  // TODO this is a bit of annoying boilderplate
  def route(msg: Any): Option[Int] = msg match {
    case acceptResponse: AcceptResponse =>
      Option(acceptResponse.to)
    case prepareResponse: PrepareResponse =>
      Option(prepareResponse.to)
    case retransmitResponse: RetransmitResponse =>
      Option(retransmitResponse.to)
    case retransmitRequest: RetransmitRequest =>
      Option(retransmitRequest.to)
    case x =>
      None // broadcast
  }

  // TODO annoying that the sender is a UDP socket so we cannot reply to it
  override def send(sender: ActorRef, msg: Any): Unit = route(msg) foreach {
    others.get(_) match {
      case Some(a) =>
        log.debug("routed {} to {}", msg, a)
        a ! msg
      case None =>
        log.warning("routed {} to {} but not found in {}", msg, route(msg), others)
    }
  }
}