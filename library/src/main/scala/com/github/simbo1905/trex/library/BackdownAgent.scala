package com.github.simbo1905.trex.library

import scala.collection.immutable.SortedMap

import Ordering._

trait BackdownAgent { this: PaxosLenses =>

  def backdownAgent(io: PaxosIO, agent: PaxosAgent): PaxosAgent = {
    if( agent.data.clientCommands.nonEmpty) io.sendNoLongerLeader(agent.data.clientCommands)
    agent.copy( role = Follower, data = backdownLens.set(agent.data, (SortedMap.empty, SortedMap.empty, Map.empty, None, io.randomTimeout)))
  }

}
