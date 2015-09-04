package com.github.simbo1905.trex.internals

import akka.actor.ActorRef

import scala.collection.SortedMap

/**
 * This algebraic type holds the state of a node in the cluster.
 *
 * @param progress The highest promised and highest committed progress of a node in the cluster.
 * @param leaderHeartbeat The last heartbeat value seen from a leader. Note that clocks are not synced so this value is only used as evidence that a stable leader is up whereas the paxos number and committed slot are taken as authoritative that a new leader is making progress.
 * @param timeout The next randomised point in time that this node will timeout. Followers timeout on Commit messages and become a Recoverer. Recoverers timeout on PrepareResponses and AcceptResponses. Leaders timeout on AcceptResponses.
 * @param clusterSize The current size of the cluster.
 * @param prepareResponses The outstanding uncommitted proposed work of the leader take over phase during the recovery of a leader failover.
 *                         Each key is an identifier of a prepare for which we are collecting a majority response to determine the highest proposed value of the previous leader if any.
 * @param epoch The leaders paxos number when leading.
 * @param acceptResponses Tracking of responses to accept messages when Recoverer or Leader. Each key is an identifier of the command we want to commit. Each value is a map of the ack/nacks of each cluster node with a timeout.
 * @param clientCommands The client work outstanding with the leader. The map key is the accept identifier and the value is a tuple of the client command and the client ref.
 */
case class PaxosData(progress: Progress,
                     leaderHeartbeat: Long,
                     timeout: Long,
                     clusterSize: Int,
                     prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]] = SortedMap.empty[Identifier, Map[Int, PrepareResponse]](Ordering.IdentifierLogOrdering),
                     epoch: Option[BallotNumber] = None,
                     acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout] = SortedMap.empty[Identifier, AcceptResponsesAndTimeout](Ordering.IdentifierLogOrdering),
                     clientCommands: Map[Identifier, (CommandValue, ActorRef)] = Map.empty)

object PaxosData {
  val prepareResponsesLens = Lens(
    get = (_: PaxosData).prepareResponses,
    set = (nodeData: PaxosData, prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]]) => nodeData.copy(prepareResponses = prepareResponses))

  val acceptResponsesLens = Lens(
    get = (_: PaxosData).acceptResponses,
    set = (nodeData: PaxosData, acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout]) => nodeData.copy(acceptResponses = acceptResponses)
  )

  val clientCommandsLens = Lens(
    get = (_: PaxosData).clientCommands,
    set = (nodeData: PaxosData, clientCommands: Map[Identifier, (CommandValue, ActorRef)]) => nodeData.copy(clientCommands = clientCommands)
  )

  val acceptResponsesClientCommandsLens = Lens(
    get = (n: PaxosData) => ((acceptResponsesLens(n), clientCommandsLens(n))),
    set = (n: PaxosData, value: (SortedMap[Identifier, AcceptResponsesAndTimeout], Map[Identifier, (CommandValue, ActorRef)])) =>
      value match {
        case (acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout], clientCommands: Map[Identifier, (CommandValue, ActorRef)]) =>
          acceptResponsesLens.set(clientCommandsLens.set(n, clientCommands), acceptResponses)
      }
  )

  val timeoutLens = Lens(
    get = (_: PaxosData).timeout,
    set = (nodeData: PaxosData, timeout: Long) => nodeData.copy(timeout = timeout)
  )

  val epochLens = Lens(
    get = (_: PaxosData).epoch,
    set = (nodeData: PaxosData, epoch: Option[BallotNumber]) => nodeData.copy(epoch = epoch)
  )

  val leaderLens = Lens(
    get = (nodeData: PaxosData) => ((prepareResponsesLens(nodeData), acceptResponsesLens(nodeData), clientCommandsLens(nodeData))),
    set = (nodeData: PaxosData, value: (SortedMap[Identifier, Map[Int, PrepareResponse]],
      SortedMap[Identifier, AcceptResponsesAndTimeout],
      Map[Identifier, (CommandValue, ActorRef)])) =>
      value match {
        case (prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]],
        acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout],
        clientCommands: Map[Identifier, (CommandValue, ActorRef)]) =>
          prepareResponsesLens.set(acceptResponsesLens.set(clientCommandsLens.set(nodeData, clientCommands), acceptResponses), prepareResponses)
      }
  )

  val backdownLens = Lens(
    get = (n: PaxosData) => ((prepareResponsesLens(n), acceptResponsesLens(n), clientCommandsLens(n), epochLens(n), timeoutLens(n))),
    set = (n: PaxosData, value: (SortedMap[Identifier, Map[Int, PrepareResponse]], SortedMap[Identifier, AcceptResponsesAndTimeout], Map[Identifier, (CommandValue, ActorRef)], Option[BallotNumber], Long)) =>
      value match {
        case
          (prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]],
          acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout],
          clientCommands: Map[Identifier, (CommandValue, ActorRef)],
          epoch: Option[BallotNumber],
          timeout: Long
            ) => prepareResponsesLens.set(acceptResponsesLens.set(clientCommandsLens.set(epochLens.set(timeoutLens.set(n, timeout), epoch), clientCommands), acceptResponses), prepareResponses)
      }
  )

  val progressLens = Lens(
    get = (_: PaxosData).progress,
    set = (nodeData: PaxosData, progress: Progress) => nodeData.copy(progress = progress)
  )

  val highestPromisedLens = progressLens andThen Lens(get = (_: Progress).highestPromised, set = (progress: Progress, promise: BallotNumber) => progress.copy(highestPromised = promise))

  val timeoutPrepareResponsesLens = Lens(
    get = (nodeData: PaxosData) => ((timeoutLens(nodeData), prepareResponsesLens(nodeData))),
    set = (nodeData: PaxosData, value: (Long, SortedMap[Identifier, Map[Int, PrepareResponse]])) =>
      value match {
        case (timeout: Long, prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]]) =>
          timeoutLens.set(prepareResponsesLens.set(nodeData, prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]]), timeout)
      }
  )

  val progressAcceptResponsesEpochTimeoutLens = Lens(
    get = (nodeData: PaxosData) => ((progressLens(nodeData), acceptResponsesLens(nodeData), epochLens(nodeData), timeoutLens(nodeData))),
    set = (nodeData: PaxosData, value: (Progress, SortedMap[Identifier, AcceptResponsesAndTimeout], Option[BallotNumber], Long)) =>
      value match {
        case (progress: Progress, acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout], epoch: Option[BallotNumber], timeout: Long) =>
          acceptResponsesLens.set(timeoutLens.set(epochLens.set(progressLens.set(nodeData, progress), epoch), timeout), acceptResponses)
      }
  )

  val highestPromisedTimeoutEpochPrepareResponsesAcceptResponseLens = Lens(
    get = (n: PaxosData) => (highestPromisedLens(n), timeoutLens(n), epochLens(n), prepareResponsesLens(n), acceptResponsesLens(n)),
    set = (n: PaxosData, value: (BallotNumber, Long, Option[BallotNumber], SortedMap[Identifier, Map[Int, PrepareResponse]], SortedMap[Identifier, AcceptResponsesAndTimeout])) =>
      value match {
        case (promise: BallotNumber, timeout: Long, epoch: Option[BallotNumber], prepareResponses: SortedMap[Identifier, Map[Int, PrepareResponse]], acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout]) =>
          highestPromisedLens.set(timeoutLens.set(epochLens.set(prepareResponsesLens.set(acceptResponsesLens.set(n, acceptResponses), prepareResponses), epoch), timeout), promise)
      }
  )
}

