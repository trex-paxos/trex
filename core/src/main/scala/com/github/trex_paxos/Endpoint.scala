package com.github.trex_paxos

import java.net.InetSocketAddress

import _root_.akka.actor.{ActorRef, PoisonPill, Props, TypedActor}
import _root_.akka.actor.TypedActor.MethodCall
import _root_.akka.serialization.SerializationExtension
import com.github.trex_paxos.internals._
import com.github.trex_paxos.library._

import scala.compat.Platform
import scala.util.Try

// TODO should this be a MethodCall mix-in rather than a concrete class
class TypedActorPaxosEndpoint(
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
    case Payload(logIndex, c: ClientCommandValue) =>
      val mc@TypedActor.MethodCall(method, parameters) = deserialize(c.bytes)
      log.debug("delivering slot {} value {}", logIndex, mc)
      val result = Try {
        val response = Option(method.invoke(target, parameters: _*))
        log.debug(s"invoked ${method.getName} returned $response")
        ServerResponse(logIndex, c.msgId, response)
      } recover {
        case ex =>
          log.error(ex, s"call to $method with $parameters got exception $ex")
          ServerResponse(logIndex, c.msgId, Option(ex))
      }
      result.get
  }

  def senders(members: Seq[Member]): Map[Int, ActorRef] = {
    val others = members.filterNot(_.nodeUniqueId == nodeUniqueId)
    log.info("{} creating senders for nodes {}", nodeUniqueId, others)
    others.map { n =>
      import Member.pattern
      val pattern(host, port) = n.location
      n.nodeUniqueId -> context.system.actorOf(Props(classOf[UdpSender],
        new java.net.InetSocketAddress(host, port.toInt)), s"UdpSender${n.nodeUniqueId}-${Platform.currentTime}")
    }.toMap
  }

  def notNoneMembership = membershipStore.loadMembership().getOrElse(CommittedMembership(Long.MinValue, Membership()))

  // FIXME this could IOError so we should move it to an Init method or the actor will stop.
  var committedMembership = notNoneMembership
  var others: Map[Int, ActorRef] = senders(committedMembership.membership.members)

  log.info(s"cluster members are ${others}")

  override val deliverMembership: PartialFunction[Payload, AnyRef] = {
    case p@Payload(logIndex, _) =>
//      val result = Try {
//        committedMembership = CommittedMembership(logIndex, membership)
//        membershipStore.saveMembership(committedMembership)
//        others.values foreach {
//          _ ! PoisonPill
//        }
//        others = senders(committedMembership.membership.members)
//        log.info(s"membership at logIndex ${logIndex} is $membership")
//        ServerResponse(logIndex, msgId, None)
//      } recover {
//        case ex =>
//          log.error(ex, s"call save membership at logIndex ${logIndex} with membership ${membership} got exception $ex")
//          ServerResponse(logIndex, msgId, Option(ex))
//      }
//      result.get
    p // FIXME
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