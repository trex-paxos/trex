package com.github.simbo1905.trex.library;

trait ReturnToFollowerHandler[ClientRef] {
   def plog: PaxosLogging

   def randomTimeout: Long

   def nodeUniqueId: Int

   def commit(state: PaxosRole, data: PaxosData[ClientRef], identifier: Identifier, progress: Progress): (Progress, Seq[(Identifier, Any)])

   def sendNoLongerLeader(clientCommands: Map[Identifier, (CommandValue, ClientRef)]): Unit

   def send(actor: ClientRef, msg: Any): Unit

   def handleReturnToFollowerOnHigherCommit(c: Commit, nodeData: PaxosData[ClientRef], stateName: PaxosRole, sender: ClientRef): Progress = {
     plog.info("Node {} {} has seen a higher commit {} from node {} so will backdown to be Follower", nodeUniqueId, stateName, c, c.identifier.from)

     val higherSlotCommit = c.identifier.logIndex > nodeData.progress.highestCommitted.logIndex

     val progress = if( higherSlotCommit ) {
       val (newProgress, _) = commit(stateName, nodeData, c.identifier, nodeData.progress)
       if ( newProgress == nodeData.progress) {
         send(sender, RetransmitRequest(nodeUniqueId, c.identifier.from, nodeData.progress.highestCommitted.logIndex))
       }
       newProgress
     } else {
       nodeData.progress
     }

     sendNoLongerLeader(nodeData.clientCommands)

     progress
   }
 }
