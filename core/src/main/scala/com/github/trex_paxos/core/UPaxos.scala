package com.github.trex_paxos.core

import com.github.trex_paxos.library.{BallotNumber, PaxosIO}

trait UPaxos { self: PaxosIO =>

  var newEraBallotNumber: Option[BallotNumber] = None

}
