package com.github.simbo1905.trex.internals

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import com.github.simbo1905.trex.Journal

import scala.collection.SortedMap
import Ordering._

trait ResendAcceptsHandler {
  def highestNumberProgressed(data: PaxosData): BallotNumber

  def nodeUniqueId: Int

  def log: LoggingAdapter

  def journal: Journal

  def broadcastRef: ActorRef

  def send(actor: ActorRef, msg: Any): Unit

  def randomTimeout: Long

  def clock: Long

  def handleResendAccepts(stateName: PaxosRole, oldData: PaxosData, time: Long): PaxosData = {
    import ResendAcceptsHandler._

    val data = PaxosData.timeoutLens.set(oldData, randomTimeout)

    val late = timedout(time, data.acceptResponses)

    if (late.isEmpty) {
      data
    } else {
      log.info(s"timed out on ${late.size} accepts")

      val highPromise = highestPromise(data.progress.highestPromised, late)

      val oldAccepts = late.map {
        case (id, AcceptResponsesAndTimeout(_, a, _)) => a
      }

      val (accepts, newData) = if (highPromise > data.epoch.getOrElse(Journal.minBookwork.highestPromised)) {
        val higherNumber = highPromise.copy(counter = highPromise.counter + 1, nodeIdentifier = nodeUniqueId)
        val newProgress = data.progress.copy(highestPromised = higherNumber)
        val removedOld = data.acceptResponses -- oldAccepts.map(_.id)
        val newAccepts = refreshAccepts(higherNumber, oldAccepts)
        val newTimeout = randomTimeout
        val newVotes = newAccepts.foldLeft(removedOld) { (responses, accept) =>
          responses + (accept.id -> AcceptResponsesAndTimeout(newTimeout, accept, Map(nodeUniqueId -> AcceptAck(accept.id, nodeUniqueId, newProgress))))
        }
        journal.save(newProgress)
        (newAccepts, PaxosData.progressAcceptResponsesEpochTimeoutLens.set(data, (newProgress, newVotes, Some(higherNumber), newTimeout)))
      } else {
        (oldAccepts, data)
      }

      accepts.foreach(send(broadcastRef, _))

      newData
    }
  }
}

object ResendAcceptsHandler {
  def timedout(time: Long, responses: SortedMap[Identifier, AcceptResponsesAndTimeout]) = responses.filter {
    case (_, AcceptResponsesAndTimeout(to, _, _)) if to >= time => true
    case _ => false
  }

  def highestPromise(ownPromise: BallotNumber, acceptResponses: SortedMap[Identifier, AcceptResponsesAndTimeout]): BallotNumber = {
    val others = acceptResponses.values.map(_.responses).flatMap(_.values) flatMap { r =>
      Seq(r.progress.highestCommitted.number, r.progress.highestPromised)
    }
    (Seq(ownPromise) ++ others).max
  }

  def refreshAccepts(newNumber: BallotNumber, accepts: Traversable[Accept]) = {
    accepts.map(a => a.copy(a.id.copy(number = newNumber)))
  }
}
