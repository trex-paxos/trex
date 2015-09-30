package com.github.simbo1905.trex.library

trait UnhandledHandler[ClientRef] {

   def stderr(message: String) = System.err.println(message)

   def handleUnhandled(io: PaxosIO[ClientRef], agent: PaxosAgent[ClientRef], msg: Any): Unit = {
     val l = s"Node ${agent.nodeUniqueId} in state ${agent.role} recieved unknown message=${msg}"
     io.plog.error(l)
     stderr(l)
   }
 }
