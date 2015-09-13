package com.github.simbo1905.trex.library

trait UnhandledHandler[ClientRef] {

   def plog: PaxosLogging

   def trace(state: PaxosRole, data: PaxosData[ClientRef], sender: ClientRef, msg: Any): Unit

   def stderr(message: String) = System.err.println(message)

   def handleUnhandled(nodeUniqueId: Int, stateName: PaxosRole, sender: ClientRef, data: PaxosData[ClientRef], msg: Any): Unit = {
     trace(stateName, data, sender, msg)
     val l = s"Node $nodeUniqueId in state $stateName recieved unknown message=$msg"
     plog.error(l)
     stderr(l)
   }
 }
