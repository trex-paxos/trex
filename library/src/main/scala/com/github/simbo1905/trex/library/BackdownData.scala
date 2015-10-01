package com.github.simbo1905.trex.library

import scala.collection.immutable.SortedMap

import Ordering._

trait BackdownData[RemoteRef] { this: PaxosLenses[RemoteRef] =>

  // TODO this could be back down agent
  def backdownData(io: PaxosIO[RemoteRef], data: PaxosData[RemoteRef]): PaxosData[RemoteRef] = {
    if( data.clientCommands.nonEmpty) io.sendNoLongerLeader(data.clientCommands)
    backdownLens.set(data, (SortedMap.empty, SortedMap.empty, Map.empty, None, io.randomTimeout))
  }

}
