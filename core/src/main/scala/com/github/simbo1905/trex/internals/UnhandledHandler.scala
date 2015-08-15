package com.github.simbo1905.trex.internals

import akka.actor.ActorRef
import akka.actor.FSM.Event
import akka.event.LoggingAdapter

trait UnhandledHandler {

  def log: LoggingAdapter

  def trace(state: PaxosRole, data: PaxosData, sender: ActorRef, msg: Any): Unit

  def stderr(message: String) = System.err.println(message)

  def handleUnhandled(nodeUniqueId: Int, stateName: PaxosRole, sender: ActorRef, e: Event[PaxosData]): Unit = {
    trace(stateName, e.stateData, sender, e.event)
    val l = s"Node $nodeUniqueId in state $stateName recieved unknown message=$e"
    log.error(l)
    stderr(l)
  }
}
