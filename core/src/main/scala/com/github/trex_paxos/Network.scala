package com.github.trex_paxos

import java.net.InetSocketAddress

import _root_.akka.actor.{Actor, ActorLogging, ActorRef}
import _root_.akka.io.Udp.CommandFailed
import _root_.akka.io.{IO, Udp}
import _root_.akka.util.ByteString
import com.github.trex_paxos.util.{ByteChain, Pickle}

// TODO do we really need a UdpSender wrapper seems like its many actors possibly talking to one "IO(Udp)" else many to
// many in which case why this intermediary.
class UdpSender(remote: InetSocketAddress) extends Actor with ActorLogging {

  import context.system

  IO(Udp) ! Udp.SimpleSender

  def receive = {
    case "status" =>
      sender() ! false // not yet ready
    case Udp.SimpleSenderReady =>
      log.info("ready to send to {}", remote)
      context.become(ready(sender()))
    case unknown => // we drop messages if we are not connected as paxos makes this safe
      log.warning("Unready UdpSender to {} dropping message {}", remote, unknown)
  }

  def ready(connection: ActorRef): Receive = {
    case "status" =>
      sender() ! true // ready
    case msg: AnyRef =>
      log.debug("sending to {} msg {}", remote, msg)
      val packed = Pickle.pack(msg).prependCrcData()
      if (packed.size > 65535) log.warning("message size > 65,535 may not fit in UDP package") // TODO
      connection ! Udp.Send(ByteString(packed.toBytes), remote) // FIXME makes two copies :-(
    case unknown =>
      log.warning("Read UdpSender dropping unknown message {}", unknown)
  }
}

// TODO do we really need this wrapper class why not talk directly to the "IO(Udp)" respondant?
class UdpListener(socket: InetSocketAddress, nextActor: ActorRef) extends Actor with ActorLogging {

  import context.system

  log.info("Binding UdpListener to {}", socket)
  IO(Udp) ! Udp.Bind(self, socket)

  def receive = {
    case "status" =>
      sender() ! false // not yet ready
    case Udp.Bound(local) =>
      log.info("Bound UdpListener to {}", socket)
      context.become(ready(sender()))
    case c: CommandFailed =>
      log.error(s"failed to bind to $socket due to {}", c)
      context.stop(self)
    case unknown =>
      log.error("unbound listener for {} cannot yet send message {}", socket, unknown.getClass)
  }

  def ready(s: ActorRef): Receive = {
    case "status" =>
      log.info("successfully bound to {}", socket)
      sender() ! true // ready
    case Udp.Received(data, remote) =>
      val checked = ByteChain(data.toArray).checkCrcData()
      val msg = Pickle.unpack(checked.iterator)
      log.debug("received from {} {}", remote, msg)
      nextActor ! msg
    case Udp.Unbind => s ! Udp.Unbind
    case Udp.Unbound => context.stop(self)
    case unknown =>
      log.error("bound listener for {} unknown message {}", socket, unknown.getClass)
  }
}
