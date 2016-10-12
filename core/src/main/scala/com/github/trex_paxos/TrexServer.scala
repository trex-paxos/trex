package com.github.trex_paxos

import java.net.InetSocketAddress

import _root_.akka.actor._
import _root_.akka.event.Logging
import com.github.trex_paxos.internals._
import com.github.trex_paxos.library._

object TrexServer {

  def apply(config: PaxosProperties,
            clazz: Class[_ <: PaxosActorNoTimeout],
            selfNode: Node,
            membershipStore: TrexMembership,
            journal: Journal,
            target: AnyRef): Props = Props(clazz,
    config,
    selfNode,
    membershipStore,
    journal,
    target)

  def apply(config: PaxosProperties,
            selfNode: Node,
            membershipStore: TrexMembership,
            journal: Journal,
            target: AnyRef): Props = Props(classOf[TypedActorPaxosEndpoint],
    config,
    selfNode,
    membershipStore,
    journal,
    target)

  def targetPropsFactory(config: PaxosProperties,
                         clazz: Class[_ <: PaxosActorNoTimeout],
                         nodeUniqueId: Int,
                         journal: Journal,
                         target: AnyRef)(broadcast: ActorRef): Props =
    Props(clazz, config, broadcast, nodeUniqueId, journal, target)

  def initializeIfEmpty(cluster: Cluster, trexMembership: TrexMembership) = {
    trexMembership.loadMembership() match {
      case None =>
        println(s"initializing cluster membership from config")
        val members: Seq[Member] = cluster.nodes map {
          case Node(nodeUniqueId, host, cport, nport) => Member(nodeUniqueId, s"${host}:${nport}", s"${host}:${cport}", Accepting)
        }
        val m = Membership(cluster.name, members)
        println(s"saving membership ${m} at logIndex Long.MinValue")
        trexMembership.saveMembership(CommittedMembership(Long.MinValue, m))
      case Some(m) =>
        println(s"loaded cluster membership is ${m}")
    }
  }
}

trait TrexRouting extends Actor with ActorLogging {

  /**
    * @return The current cluster membership some of which may be passive observers.
    */
  def peers: Map[Int, ActorRef]

  /**
    * @return Inbound traffic originates from our network listener which is typically a UDP listener.
    */
  def networkListener: ActorRef

  /**
    * @return Outbound traffic originals from this paxos actor.
    */
  def paxosActor: ActorRef

  override def receive: Receive = {
    case outbound: AnyRef if sender == paxosActor =>
      route(outbound) match {
        case Some(nodeId) =>
          val other = peers.get(nodeId)
          log.debug("other is {} from peers {} for msg {}", other, peers, outbound)
          other.foreach(_ ! outbound)
        case _ => peers.values.foreach(_ ! outbound)
      }
    case inbound: AnyRef if sender == networkListener =>
      paxosActor ! inbound
    case t: Terminated =>
      // FIXME handle this
      log.warning(s"Termination notice $t")
    case unknown =>
      log.warning("{} unknown message {} from {}", this.getClass.getCanonicalName, unknown, sender())
  }

  // TODO this is a bit of annoying boilderplate
  def route(msg: AnyRef): Option[Int] = msg match {
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
}

/**
  * Cluster membership durable store.
  */
trait TrexMembership {
  def saveMembership(cm: CommittedMembership): Unit

  def loadMembership(): Option[CommittedMembership]
}

private[trex_paxos] class TrexServer(membershipStore: TrexMembership,
                                     selfNode: Node,
                                     config: PaxosProperties,
                                     nodeUniqueId: Int,
                                     journal: Journal,
                                     target: AnyRef)
  extends TrexRouting {

  val listenerRef = context.system.actorOf(Props(classOf[UdpListener],
  new InetSocketAddress(selfNode.host, selfNode.nodePort), self))

  override def networkListener: ActorRef = listenerRef

  /**
    * @return The current cluster membership some of which may be passive observers.
    */
  override def peers: Map[Int, ActorRef] = ???

  /**
    * @return Outbound traffic originals from this paxos actor.
    */
  override def paxosActor: ActorRef = ???
}