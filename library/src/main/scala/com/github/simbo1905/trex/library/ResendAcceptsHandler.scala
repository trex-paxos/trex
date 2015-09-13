package com.github.simbo1905.trex.library

import com.github.simbo1905.trex.library.Ordering._

import scala.collection.SortedMap

case class AcceptsAndData[ClientRef](accepts: Traversable[Accept], data: PaxosData[ClientRef])

trait ResendAcceptsHandler[ClientRef] extends PaxosLenses[ClientRef] {

  import ResendAcceptsHandler._

  def highestNumberProgressed(data: PaxosData[ClientRef]): BallotNumber

  def nodeUniqueId: Int

  def plog: PaxosLogging

  def journal: Journal

  def broadcast(msg: Any): Unit

  def randomTimeout: Long

  /**
   * Locates the accepts where we have timed-out on getting a majority accept response. If we have seen evidence of
   * other nodes using a higher BallotNumber we "go higher" moving to a new epoch and refreshing our accepts to use the
   * new ballot number.
   * @param stateName Current PaxosState for logging purpose.
   * @param oldData The current PaxosData
   * @param time The current time
   * @return
   */
  def handleResendAccepts(stateName: PaxosRole, oldData: PaxosData[ClientRef], time: Long): PaxosData[ClientRef] = {
    // compute the timed out accepts, making fresh ones with higher numbers on a new epoch if required
    val AcceptsAndData(accepts, newData) = computeResendAccepts(stateName, oldData, time)
    // if we have bumped the epoch we need to save fresh accepts and journal the new procis
    if (newData.epoch != oldData.epoch) {
      journal.save(newData.progress)
      journal.accept(accepts.toSeq: _*)
    }
    // broadcast the requests
    accepts.foreach(broadcast(_))
    newData
  }

  def computeResendAccepts(stateName: PaxosRole, oldData: PaxosData[ClientRef], time: Long): AcceptsAndData[ClientRef] = {

    val oldEpoch: BallotNumber = oldData.epoch.getOrElse(Journal.minBookwork.highestPromised)

    val newTimeout = randomTimeout

    // update the top level timeout which is a fast check guard on timeouts
    val data = timeoutLens.set(oldData, randomTimeout)

    // find the individual responses that have timed out
    val late = timedOutResponse(time, data.acceptResponses)

    if (late.isEmpty) {
      // nothing more to do
      AcceptsAndData[ClientRef](Seq.empty, data)
    } else {
      plog.info(s"timed out on ${late.size} accepts")

      // the accepts we timed out on responses for
      val oldAccepts = late.map {
        case (id, AcceptResponsesAndTimeout(_, a, _)) => a
      }

      // the highest promise we have seen in any responses
      val highPromise = highestPromise(data.progress.highestPromised, late)

      // go one higher if response show higher promises than our epoch
      val (higherNumber, newProgress) = if (highPromise > oldEpoch) {
        plog.info(s"going one higher than highest other promise ${highPromise}")
        // increment
        val higherNumber = highPromise.copy(counter = highPromise.counter + 1, nodeIdentifier = nodeUniqueId)
        // make a self promise
        val newProgress = data.progress.copy(highestPromised = higherNumber)
        (higherNumber, newProgress)
      } else {
        (oldEpoch, oldData.progress)
      }

      // no longer track the responses to the old accepts
      val removedOld = data.acceptResponses -- oldAccepts.map(_.id)
      // create new accepts with a higher promise
      val newAccepts = refreshAccepts(higherNumber, oldAccepts)
      // accept own higher accepts
      val newVotes = newAccepts.foldLeft(removedOld) { (responses, accept) =>
        responses + (accept.id -> AcceptResponsesAndTimeout(newTimeout, accept, Map(nodeUniqueId -> AcceptAck(accept.id, nodeUniqueId, newProgress))))
      }
      // set a new epoch, top level timeout, progress and responses
      AcceptsAndData(newAccepts, progressAcceptResponsesEpochTimeoutLens.set(data, (newProgress, newVotes, Some(higherNumber), newTimeout)))
    }
  }
}

object ResendAcceptsHandler {
  def timedOutResponse(time: Long, responses: SortedMap[Identifier, AcceptResponsesAndTimeout]) = responses.filter {
    case (_, AcceptResponsesAndTimeout(to, _, _)) if to <= time => true
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
