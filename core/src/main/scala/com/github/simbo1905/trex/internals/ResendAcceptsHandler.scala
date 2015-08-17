package com.github.simbo1905.trex.internals

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import com.github.simbo1905.trex.Journal

import scala.collection.SortedMap

trait ResendAcceptsHandler {
  def highestNumberProgressed(data: PaxosData): BallotNumber

  def nodeUniqueId: Int

  def log: LoggingAdapter

  def journal: Journal

  def broadcastRef: ActorRef

  def send(actor: ActorRef, msg: Any): Unit

  def randomTimeout: Long

  // FIXME set the fresh timeout when send accepts but given leader sends accepts for clients we need to timeout on individual accepts

  def handleResendAccepts(stateName: PaxosRole, data: PaxosData): PaxosData = {
    import Ordering._
    // accepts didn't get a majority yes/no and saw no higher commits so we increment the ballot number and broadcast
    val newData = data.acceptResponses match {
      case acceptResponses if acceptResponses.nonEmpty =>
        // the slots which have not yet committed
        val indexes = acceptResponses.keys.map(_.logIndex)
        // highest promised or committed at this node
        val highestLocal: BallotNumber = highestNumberProgressed(data)
        // numbers in any responses including our own possibly stale self response
        val proposalNumbers = (acceptResponses.values.map(_.responses).flatMap(_.values) flatMap { r =>
              Set(r.progress.highestCommitted.number, r.progress.highestPromised)
        }) // TODO for-comprehension?
      // the max known
      val maxNumber = (proposalNumbers.toSeq :+ highestLocal).max
        // check whether we were actively rejected
        if (maxNumber > data.epoch.get) {
          val higherNumber = maxNumber.copy(counter = maxNumber.counter + 1, nodeIdentifier = nodeUniqueId) // TODO lens?
          log.info(s"Node $nodeUniqueId {} time-out on accept old epoch {} new epoch {}", stateName, maxNumber, data.epoch.get, higherNumber)

          val freshAccepts = indexes map { slot =>
            val oldAccept = journal.accepted(slot)

            // TODO: We're making a naked get on the Option here. What if it is None? Need better handling.
            Accept(Identifier(nodeUniqueId, higherNumber, slot), oldAccept.get.value)
          }
          log.info("Node {} {} time-out on {} accepts", nodeUniqueId, stateName, freshAccepts.size)
          freshAccepts foreach { a =>
            journal.accept(a)
            send(broadcastRef, a)
          }
          PaxosData.acceptResponsesEpochTimeoutLens.set(data, (SortedMap.empty, Some(higherNumber), data.timeout))
        } else {
          log.info("Node {} {} time-out on accepts same epoch {} resending {}", nodeUniqueId, stateName, data.epoch.get, indexes)
          indexes foreach {
            journal.accepted(_) foreach {
              send(broadcastRef, _)
            }
          }
          data
        }
      case _ =>
        data
    }

    PaxosData.timeoutLens.set(newData, randomTimeout)
  }
}
