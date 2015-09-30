package com.github.simbo1905.trex.library

import scala.collection.immutable.SortedMap

import Ordering._

trait BackdownData[ClientRef] { this: PaxosLenses[ClientRef] =>

  def backdownData(io: PaxosIO[ClientRef], data: PaxosData[ClientRef]): PaxosData[ClientRef] = {
    if( data.clientCommands.nonEmpty) io.sendNoLongerLeader(data.clientCommands)
    backdownLens.set(data, (SortedMap.empty, SortedMap.empty, Map.empty, None, io.randomTimeout))
  }

}
