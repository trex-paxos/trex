package com.github.simbo1905.trex.library

import com.github.simbo1905.trex.library.Ordering._

import scala.collection.SortedMap

case class AcceptsAndData(accepts: Traversable[Accept], data: PaxosData)

trait ResendHandler extends PaxosLenses {

  import ResendHandler._

  def highestNumberProgressed(data: PaxosData): BallotNumber = Seq(data.epoch, Option(data.progress.highestPromised), Option(data.progress.highestCommitted.number)).flatten.max

  /**
   * Locates the accepts where we have timed-out on getting a majority accept response. If we have seen evidence of
   * other nodes using a higher BallotNumber we "go higher" moving to a new epoch and refreshing our accepts to use the
   * new ballot number.
   * @param io input and output
   * @param agent The current role and state
   * @param time The current time
   * @return
   */
  def handleResendAccepts(io: PaxosIO, agent: PaxosAgent, time: Long): PaxosAgent = {
    // compute the timed out accepts, making fresh ones with higher numbers on a new epoch if required
    val AcceptsAndData(accepts, newData) = computeResendAccepts(io, agent, time)
    // if we have bumped the epoch we need to save fresh accepts and journal the new progress
    if (newData.epoch != agent.data.epoch) {
      io.journal.save(newData.progress)
      io.journal.accept(accepts.toSeq: _*)
    }
    // broadcast the requests
    accepts.foreach(io.send(_))
    agent.copy(data = newData)
  }

  def handleResendPrepares(io: PaxosIO, agent: PaxosAgent, time: Long): PaxosAgent = {
    agent.data.prepareResponses foreach {
      case (id, _) =>
        io.send(Prepare(id))
    }
    agent.copy(data = timeoutLens.set(agent.data, io.randomTimeout))
  }

  def computeResendAccepts(io: PaxosIO, agent: PaxosAgent, time: Long): AcceptsAndData = {

    // find the individual responses that have timed out
    val late = timedOutResponse(time, agent.data.acceptResponses)

    if (late.isEmpty) {
      // nothing more to do
      AcceptsAndData(Seq.empty, agent.data)
    } else {
      io.plog.info(s"timed out on ${late.size} accepts")

      val oldEpoch: BallotNumber = agent.data.epoch.getOrElse(Journal.minBookwork.highestPromised)

      val newTimeout = io.randomTimeout

      // update the top level timeout which is a fast check guard on timeouts
      val data = timeoutLens.set(agent.data, io.randomTimeout)

      // the accepts we timed out on responses for
      val oldAccepts = late.map {
        case (id, AcceptResponsesAndTimeout(_, a, _)) => a
      }

      // the highest promise we have seen in any responses
      val highPromise = highestPromise(data.progress.highestPromised, late)

      // go one higher if response show higher promises than our epoch
      val (higherNumber, newProgress) = if (highPromise > oldEpoch) {
        io.plog.info(s"going one higher than highest other promise ${highPromise}")
        // increment
        val higherNumber = highPromise.copy(counter = highPromise.counter + 1, nodeIdentifier = agent.nodeUniqueId)
        // make a self promise
        val newProgress = data.progress.copy(highestPromised = higherNumber)
        (higherNumber, newProgress)
      } else {
        (oldEpoch, agent.data.progress)
      }

      // no longer track the responses to the old accepts
      val removedOld = data.acceptResponses -- oldAccepts.map(_.id)
      // create new accepts with a higher promise
      val newAccepts = refreshAccepts(higherNumber, oldAccepts)
      // accept own higher accepts
      val newVotes = newAccepts.foldLeft(removedOld) { (responses, accept) =>
        responses + (accept.id -> AcceptResponsesAndTimeout(newTimeout, accept, Map(agent.nodeUniqueId -> AcceptAck(accept.id, agent.nodeUniqueId, newProgress))))
      }
      // set a new epoch, top level timeout, progress and responses
      AcceptsAndData(newAccepts, progressAcceptResponsesEpochTimeoutLens.set(data, (newProgress, newVotes, Some(higherNumber), newTimeout)))
    }
  }
}

object ResendHandler {
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
