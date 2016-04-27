package com.github.trex_paxos

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.io.Udp.CommandFailed
import akka.io.{IO, Udp}
import com.github.trex_paxos.internals.Pickle

class UdpSender(remote: InetSocketAddress) extends Actor with ActorLogging {

  import context.system

  IO(Udp) ! Udp.SimpleSender

  def receive = {
    case "status" =>
      sender ! false // not yet ready
    case Udp.SimpleSenderReady =>
      log.info("ready to send")
      context.become(ready(sender()))
    case unknown => // we drop messages if we are not connected as paxos makes this safe
      log.warning("Unready UdpSender to {} dropping message {}", remote, unknown)
  }

  def ready(connection: ActorRef): Receive = {
    case "status" =>
      sender ! true // ready
    case msg: AnyRef =>
      log.debug("sending to {} msg {}", remote, msg)
      val packed = Pickle.pack(msg)
      if (packed.size > 65535) log.warning("message size > 65,535 may not fit in UDP package") // TODO
      connection ! Udp.Send(Pickle.pack(msg), remote) // makes defensive copy
    case unknown =>
      log.warning("Read UdpSender dropping unknown message {}", unknown)
  }
}

class UdpListener(socket: InetSocketAddress, nextActor: ActorRef) extends Actor with ActorLogging {

  import context.system

  log.info("Binding UdpListener to {}", socket)
  IO(Udp) ! Udp.Bind(self, socket)

  def receive = {
    case "status" =>
      sender ! false // not yet ready
    case Udp.Bound(local) =>
      log.info("Bound UdpListener to {}", socket)
      context.become(ready(sender()))
    case c: CommandFailed =>
      log.error(s"failed to bind to $socket due to {}", c)
      context.stop(self)
    case unknown =>
      log.error("bound listener for {} unknown message {}", socket, unknown.getClass)
  }

  def ready(s: ActorRef): Receive = {
    case "status" =>
      log.info("successfully bound to {}", socket)
      sender ! true // ready
    case Udp.Received(data, remote) =>
      val msg = Pickle.unpack(data)
      log.debug("received from {} {}", remote, msg)
      nextActor ! msg
    case Udp.Unbind => s ! Udp.Unbind
    case Udp.Unbound => context.stop(self)
    case unknown =>
      log.error("bound listener for {} unknown message {}", socket, unknown.getClass)
  }
}
