package com.github.trex_paxos.library

import scala.collection.immutable.SortedMap

import Ordering._

trait BackdownAgent { this: PaxosLenses =>

  def backdownAgent(io: PaxosIO, agent: PaxosAgent): PaxosAgent = {
    io.logger.info("Node {} is backing down", agent.nodeUniqueId)
    // tell any waiting clients that we are no longer leader so have no results for them
    io.respond(None)
    agent.copy( role = Follower, data = backdownLens.set(agent.data, (SortedMap.empty, SortedMap.empty, None, io.scheduleRandomCheckTimeout)))
  }

}
