package com.github.trex_paxos

import java.net.InetSocketAddress

import akka.actor._
import akka.event.Logging
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

  def targetPropsFactory(config: PaxosProperties,
                         clazz: Class[_ <: PaxosActorNoTimeout],
                         clusterSize: () => Int,
                         nodeUniqueId: Int,
                         journal: Journal,
                         target: AnyRef)(broadcast: ActorRef): Props =
    Props(clazz, config, clusterSize, broadcast, nodeUniqueId, journal, target)

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

object TrexStaticMembershipServer {
  def apply(cluster: Cluster,
            config: PaxosProperties,
            nodeUniqueId: Int,
            journal: Journal,
            target: AnyRef): Props = Props.create(classOf[TrexStaticMembershipServer], cluster, config, int2Integer(nodeUniqueId), journal, target)
}

private[trex_paxos] class TrexStaticMembershipServer(cluster: Cluster,
                                                     config: PaxosProperties,
                                                     nodeUniqueId: Int,
                                                     journal: Journal,
                                                     target: AnyRef)
  extends TrexRouting {

  val selfNode = cluster.nodeMap(nodeUniqueId)

  def senders(system: ActorSystem, nodes: Seq[Node]): Map[Int, ActorRef] = {
    val log = Logging.getLogger(system, this)
    log.info("{} creating senders for nodes {}", selfNode, nodes)
    nodes.map { n =>
      n.nodeUniqueId -> system.actorOf(Props(classOf[UdpSender],
        new java.net.InetSocketAddress(n.host, n.nodePort)), s"UdpSender${n.nodeUniqueId}")
    }.toMap
  }

  val others = senders(context.system, cluster.nodes.filterNot(_.nodeUniqueId == nodeUniqueId))

  override def peers: Map[Int, ActorRef] = others

  val listenerRef = context.system.actorOf(Props(classOf[UdpListener],
    new InetSocketAddress(selfNode.host, selfNode.nodePort), self), s"UdpListener${selfNode.nodeUniqueId}")

  override def networkListener: ActorRef = listenerRef

  def targetProps: (ActorRef) => Props =
    TrexServer.targetPropsFactory(config,
      classOf[TypedActorPaxosEndpoint],
      cluster.nodeMap.size _,
      nodeUniqueId,
      journal,
      target)

  val targetActorRef: ActorRef = context.system.actorOf(targetProps(self), "PaxosActor")

  override def paxosActor: ActorRef = targetActorRef
}

/**
  * Cluster membership durable store.
  */
trait TrexMembership {
  def saveMembership(slot: Long, membership: Membership): Unit

  def loadMembership(): Option[Membership]
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