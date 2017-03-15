package com.github.trex_paxos.core

import com.github.trex_paxos.library._

import scala.collection.mutable.ArrayBuffer

trait CollectingPaxosIO { self: PaxosIO =>

  val outbound = ArrayBuffer[PaxosMessage]()

  /**
    * Send a paxos algorithm message within the cluster. May be deferred.
    */
  override def send(msg: PaxosMessage): Unit = outbound += msg

}