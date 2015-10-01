package com.github.simbo1905.trex.library

import scala.collection.immutable.SortedMap

trait PaxosLenses[RemoteRef] {
   val timeoutLens = Lens(
     get = (_: PaxosData[RemoteRef]).timeout,
     set = (nodeData: PaxosData[RemoteRef], timeout: Long) => nodeData.copy(timeout = timeout)
   )

   val progressLens = Lens(
     get = (_: PaxosData[RemoteRef]).progress,
     set = (nodeData: PaxosData[RemoteRef], progress: Progress) => nodeData.copy(progress = progress)
   )

   val prepareResponsesLens = Lens(
     get = (_: PaxosData[RemoteRef]).prepareResponses,
     set = (nodeData: PaxosData[RemoteRef], prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]]) => nodeData.copy(prepareResponses = prepareResponses))


   val acceptResponsesLens = Lens(
     get = (_: PaxosData[RemoteRef]).acceptResponses,
     set = (nodeData: PaxosData[RemoteRef], acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout]) => nodeData.copy(acceptResponses = acceptResponses)
   )

   val epochLens = Lens(
     get = (_: PaxosData[RemoteRef]).epoch,
     set = (nodeData: PaxosData[RemoteRef], epoch: Option[BallotNumber]) => nodeData.copy(epoch = epoch)
   )

   val progressAcceptResponsesEpochTimeoutLens = Lens(
     get = (nodeData: PaxosData[RemoteRef]) => ((progressLens(nodeData), acceptResponsesLens(nodeData), epochLens(nodeData), timeoutLens(nodeData))),
     set = (nodeData: PaxosData[RemoteRef], value: (Progress, SortedMap[Identifier, AcceptResponsesAndTimeout], Option[BallotNumber], Long)) =>
       value match {
         case (progress: Progress, acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout], epoch: Option[BallotNumber], timeout: Long) =>
           acceptResponsesLens.set(timeoutLens.set(epochLens.set(progressLens.set(nodeData, progress), epoch), timeout), acceptResponses)
       }
   )

   val timeoutPrepareResponsesLens = Lens(
     get = (nodeData: PaxosData[RemoteRef]) => ((timeoutLens(nodeData), prepareResponsesLens(nodeData))),
     set = (nodeData: PaxosData[RemoteRef], value: (Long, SortedMap[Identifier, Map[Int, PrepareResponse]])) =>
       value match {
         case (timeout: Long, prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]]) =>
           timeoutLens.set(prepareResponsesLens.set(nodeData, prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]]), timeout)
       }
   )

   val highestPromisedLens = progressLens andThen Lens(get = (_: Progress).highestPromised, set = (progress: Progress, promise: BallotNumber) => progress.copy(highestPromised = promise))

   val highestPromisedTimeoutEpochPrepareResponsesAcceptResponseLens = Lens(
     get = (n: PaxosData[RemoteRef]) => (highestPromisedLens(n), timeoutLens(n), epochLens(n), prepareResponsesLens(n), acceptResponsesLens(n)),
     set = (n: PaxosData[RemoteRef], value: (BallotNumber, Long, Option[BallotNumber], SortedMap[Identifier, Map[Int, PrepareResponse]], SortedMap[Identifier, AcceptResponsesAndTimeout])) =>
       value match {
         case (promise: BallotNumber, timeout: Long, epoch: Option[BallotNumber], prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]], acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout]) =>
           highestPromisedLens.set(timeoutLens.set(epochLens.set(prepareResponsesLens.set(acceptResponsesLens.set(n, acceptResponses), prepareResponses), epoch), timeout), promise)
       }
   )

   val clientCommandsLens = Lens(
     get = (_: PaxosData[RemoteRef]).clientCommands,
     set = (nodeData: PaxosData[RemoteRef], clientCommands: Map[Identifier, (CommandValue, RemoteRef)]) => nodeData.copy(clientCommands = clientCommands)
   )

   val leaderLens = Lens(
     get = (nodeData: PaxosData[RemoteRef]) => ((prepareResponsesLens(nodeData), acceptResponsesLens(nodeData), clientCommandsLens(nodeData))),
     set = (nodeData: PaxosData[RemoteRef], value: (SortedMap[Identifier, Map[Int, PrepareResponse]],
       SortedMap[Identifier, AcceptResponsesAndTimeout],
       Map[Identifier, (CommandValue, RemoteRef)])) =>
       value match {
         case (prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]],
         acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout],
         clientCommands: Map[Identifier, (CommandValue, RemoteRef)]) =>
           prepareResponsesLens.set(acceptResponsesLens.set(clientCommandsLens.set(nodeData, clientCommands), acceptResponses), prepareResponses)
       }
   )

  val backdownLens = Lens(
    get = (n: PaxosData[RemoteRef]) => ((prepareResponsesLens(n), acceptResponsesLens(n), clientCommandsLens(n), epochLens(n), timeoutLens(n))),
    set = (n: PaxosData[RemoteRef], value: (SortedMap[Identifier, Map[Int, PrepareResponse]], SortedMap[Identifier, AcceptResponsesAndTimeout], Map[Identifier, (CommandValue, RemoteRef)], Option[BallotNumber], Long)) =>
      value match {
        case
          (prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]],
          acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout],
          clientCommands: Map[Identifier, (CommandValue, RemoteRef)],
          epoch: Option[BallotNumber],
          timeout: Long
            ) => prepareResponsesLens.set(acceptResponsesLens.set(clientCommandsLens.set(epochLens.set(timeoutLens.set(n, timeout), epoch), clientCommands), acceptResponses), prepareResponses)
      }
  )

  val acceptResponsesClientCommandsLens = Lens(
    get = (n: PaxosData[RemoteRef]) => ((acceptResponsesLens(n), clientCommandsLens(n))),
    set = (n: PaxosData[RemoteRef], value: (SortedMap[Identifier, AcceptResponsesAndTimeout], Map[Identifier, (CommandValue, RemoteRef)])) =>
      value match {
        case (acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout], clientCommands: Map[Identifier, (CommandValue, RemoteRef)]) =>
          acceptResponsesLens.set(clientCommandsLens.set(n, clientCommands), acceptResponses)
      }
  )
 }
