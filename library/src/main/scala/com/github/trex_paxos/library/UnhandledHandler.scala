package com.github.trex_paxos.library

trait UnhandledHandler {

   def stderr(message: String) = System.err.println(message)

   def handleUnhandled(io: PaxosIO, agent: PaxosAgent, msg: Any): Unit = {
     val l = s"Node ${agent.nodeUniqueId} in state ${agent.role} recieved unknown message=${msg}"
     io.plog.error(l)
     stderr(l)
   }
 }
