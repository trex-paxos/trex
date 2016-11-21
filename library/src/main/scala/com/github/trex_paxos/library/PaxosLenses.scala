package com.github.trex_paxos.library

import scala.collection.immutable.SortedMap

trait PaxosLenses {
   val timeoutLens = Lens(
     get = (_: PaxosData).timeout,
     set = (nodeData: PaxosData, timeout: Long) => nodeData.copy(timeout = timeout)
   )

   val progressLens = Lens(
     get = (_: PaxosData).progress,
     set = (nodeData: PaxosData, progress: Progress) => nodeData.copy(progress = progress)
   )

   val prepareResponsesLens = Lens(
     get = (_: PaxosData).prepareResponses,
     set = (nodeData: PaxosData, prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]]) => nodeData.copy(prepareResponses = prepareResponses))


   val acceptResponsesLens = Lens(
     get = (_: PaxosData).acceptResponses,
     set = (nodeData: PaxosData, acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout]) => nodeData.copy(acceptResponses = acceptResponses)
   )

   val epochLens = Lens(
     get = (_: PaxosData).epoch,
     set = (nodeData: PaxosData, epoch: Option[BallotNumber]) => nodeData.copy(epoch = epoch)
   )

   val progressAcceptResponsesEpochTimeoutLens = Lens(
     get = (nodeData: PaxosData) => ((progressLens(nodeData), acceptResponsesLens(nodeData), epochLens(nodeData), timeoutLens(nodeData))),
     set = (nodeData: PaxosData, value: (Progress, SortedMap[Identifier, AcceptResponsesAndTimeout], Option[BallotNumber], Long)) =>
       value match {
         case (progress: Progress, acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout], epoch: Option[BallotNumber], timeout: Long) =>
           acceptResponsesLens.set(timeoutLens.set(epochLens.set(progressLens.set(nodeData, progress), epoch), timeout), acceptResponses)
       }
   )

   val timeoutPrepareResponsesLens = Lens(
     get = (nodeData: PaxosData) => ((timeoutLens(nodeData), prepareResponsesLens(nodeData))),
     set = (nodeData: PaxosData, value: (Long, SortedMap[Identifier, Map[Int, PrepareResponse]])) =>
       value match {
         case (timeout: Long, prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]]) =>
           timeoutLens.set(prepareResponsesLens.set(nodeData, prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]]), timeout)
       }
   )

   val highestPromisedLens = progressLens andThen Lens(get = (_: Progress).highestPromised, set = (progress: Progress, promise: BallotNumber) => progress.copy(highestPromised = promise))

   val highestPromisedTimeoutEpochPrepareResponsesAcceptResponseLens = Lens(
     get = (n: PaxosData) => (highestPromisedLens(n), timeoutLens(n), epochLens(n), prepareResponsesLens(n), acceptResponsesLens(n)),
     set = (n: PaxosData, value: (BallotNumber, Long, Option[BallotNumber], SortedMap[Identifier, Map[Int, PrepareResponse]], SortedMap[Identifier, AcceptResponsesAndTimeout])) =>
       value match {
         case (promise: BallotNumber, timeout: Long, epoch: Option[BallotNumber], prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]], acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout]) =>
           highestPromisedLens.set(timeoutLens.set(epochLens.set(prepareResponsesLens.set(acceptResponsesLens.set(n, acceptResponses), prepareResponses), epoch), timeout), promise)
       }
   )

   val leaderLens = Lens(
     get = (nodeData: PaxosData) => ((prepareResponsesLens(nodeData), acceptResponsesLens(nodeData))),
     set = (nodeData: PaxosData, value: (SortedMap[Identifier, Map[Int, PrepareResponse]],
       SortedMap[Identifier, AcceptResponsesAndTimeout]
       )
           ) =>
       value match {
         case (prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]],
         acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout]
           ) =>
           prepareResponsesLens.set(acceptResponsesLens.set(nodeData, acceptResponses), prepareResponses)

       }
   )

  val backdownLens = Lens(
    get = (n: PaxosData) => ((prepareResponsesLens(n), acceptResponsesLens(n), epochLens(n), timeoutLens(n))),
    set = (n: PaxosData, value: (SortedMap[Identifier, Map[Int, PrepareResponse]], SortedMap[Identifier, AcceptResponsesAndTimeout], Option[BallotNumber], Long)) =>
      value match {
        case
          (prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]],
          acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout],
          epoch: Option[BallotNumber],
          timeout: Long
            ) => prepareResponsesLens.set(acceptResponsesLens.set(epochLens.set(timeoutLens.set(n, timeout), epoch), acceptResponses), prepareResponses)
      }
  )
 }
