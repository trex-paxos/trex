package com.github.simbo1905.trex.library

import scala.collection.immutable.SortedMap

trait PaxosLenses[ClientRef] {
   val timeoutLens = Lens(
     get = (_: PaxosData[ClientRef]).timeout,
     set = (nodeData: PaxosData[ClientRef], timeout: Long) => nodeData.copy(timeout = timeout)
   )

   val progressLens = Lens(
     get = (_: PaxosData[ClientRef]).progress,
     set = (nodeData: PaxosData[ClientRef], progress: Progress) => nodeData.copy(progress = progress)
   )

   val prepareResponsesLens = Lens(
     get = (_: PaxosData[ClientRef]).prepareResponses,
     set = (nodeData: PaxosData[ClientRef], prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]]) => nodeData.copy(prepareResponses = prepareResponses))


   val acceptResponsesLens = Lens(
     get = (_: PaxosData[ClientRef]).acceptResponses,
     set = (nodeData: PaxosData[ClientRef], acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout]) => nodeData.copy(acceptResponses = acceptResponses)
   )

   val epochLens = Lens(
     get = (_: PaxosData[ClientRef]).epoch,
     set = (nodeData: PaxosData[ClientRef], epoch: Option[BallotNumber]) => nodeData.copy(epoch = epoch)
   )

   val progressAcceptResponsesEpochTimeoutLens = Lens(
     get = (nodeData: PaxosData[ClientRef]) => ((progressLens(nodeData), acceptResponsesLens(nodeData), epochLens(nodeData), timeoutLens(nodeData))),
     set = (nodeData: PaxosData[ClientRef], value: (Progress, SortedMap[Identifier, AcceptResponsesAndTimeout], Option[BallotNumber], Long)) =>
       value match {
         case (progress: Progress, acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout], epoch: Option[BallotNumber], timeout: Long) =>
           acceptResponsesLens.set(timeoutLens.set(epochLens.set(progressLens.set(nodeData, progress), epoch), timeout), acceptResponses)
       }
   )

   val timeoutPrepareResponsesLens = Lens(
     get = (nodeData: PaxosData[ClientRef]) => ((timeoutLens(nodeData), prepareResponsesLens(nodeData))),
     set = (nodeData: PaxosData[ClientRef], value: (Long, SortedMap[Identifier, Map[Int, PrepareResponse]])) =>
       value match {
         case (timeout: Long, prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]]) =>
           timeoutLens.set(prepareResponsesLens.set(nodeData, prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]]), timeout)
       }
   )

   val highestPromisedLens = progressLens andThen Lens(get = (_: Progress).highestPromised, set = (progress: Progress, promise: BallotNumber) => progress.copy(highestPromised = promise))

   val highestPromisedTimeoutEpochPrepareResponsesAcceptResponseLens = Lens(
     get = (n: PaxosData[ClientRef]) => (highestPromisedLens(n), timeoutLens(n), epochLens(n), prepareResponsesLens(n), acceptResponsesLens(n)),
     set = (n: PaxosData[ClientRef], value: (BallotNumber, Long, Option[BallotNumber], SortedMap[Identifier, Map[Int, PrepareResponse]], SortedMap[Identifier, AcceptResponsesAndTimeout])) =>
       value match {
         case (promise: BallotNumber, timeout: Long, epoch: Option[BallotNumber], prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]], acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout]) =>
           highestPromisedLens.set(timeoutLens.set(epochLens.set(prepareResponsesLens.set(acceptResponsesLens.set(n, acceptResponses), prepareResponses), epoch), timeout), promise)
       }
   )

   val clientCommandsLens = Lens(
     get = (_: PaxosData[ClientRef]).clientCommands,
     set = (nodeData: PaxosData[ClientRef], clientCommands: Map[Identifier, (CommandValue, ClientRef)]) => nodeData.copy(clientCommands = clientCommands)
   )

   val leaderLens = Lens(
     get = (nodeData: PaxosData[ClientRef]) => ((prepareResponsesLens(nodeData), acceptResponsesLens(nodeData), clientCommandsLens(nodeData))),
     set = (nodeData: PaxosData[ClientRef], value: (SortedMap[Identifier, Map[Int, PrepareResponse]],
       SortedMap[Identifier, AcceptResponsesAndTimeout],
       Map[Identifier, (CommandValue, ClientRef)])) =>
       value match {
         case (prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]],
         acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout],
         clientCommands: Map[Identifier, (CommandValue, ClientRef)]) =>
           prepareResponsesLens.set(acceptResponsesLens.set(clientCommandsLens.set(nodeData, clientCommands), acceptResponses), prepareResponses)
       }
   )

  val backdownLens = Lens(
    get = (n: PaxosData[ClientRef]) => ((prepareResponsesLens(n), acceptResponsesLens(n), clientCommandsLens(n), epochLens(n), timeoutLens(n))),
    set = (n: PaxosData[ClientRef], value: (SortedMap[Identifier, Map[Int, PrepareResponse]], SortedMap[Identifier, AcceptResponsesAndTimeout], Map[Identifier, (CommandValue, ClientRef)], Option[BallotNumber], Long)) =>
      value match {
        case
          (prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]],
          acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout],
          clientCommands: Map[Identifier, (CommandValue, ClientRef)],
          epoch: Option[BallotNumber],
          timeout: Long
            ) => prepareResponsesLens.set(acceptResponsesLens.set(clientCommandsLens.set(epochLens.set(timeoutLens.set(n, timeout), epoch), clientCommands), acceptResponses), prepareResponses)
      }
  )

  val acceptResponsesClientCommandsLens = Lens(
    get = (n: PaxosData[ClientRef]) => ((acceptResponsesLens(n), clientCommandsLens(n))),
    set = (n: PaxosData[ClientRef], value: (SortedMap[Identifier, AcceptResponsesAndTimeout], Map[Identifier, (CommandValue, ClientRef)])) =>
      value match {
        case (acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout], clientCommands: Map[Identifier, (CommandValue, ClientRef)]) =>
          acceptResponsesLens.set(clientCommandsLens.set(n, clientCommands), acceptResponses)
      }
  )
 }
