package com.github.trex_paxos.library

object Ordering {

  implicit object IdentifierLogOrdering extends Ordering[Identifier] {
    def compare(o1: Identifier, o2: Identifier) = if (o1.logIndex == o2.logIndex) 0 else if (o1.logIndex >= o2.logIndex) 1 else -1
  }

  implicit object BallotNumberOrdering extends Ordering[BallotNumber] {
    def compare(n1: BallotNumber, n2: BallotNumber) = if (n1 == n2) 0 else if (n1 > n2) 1 else -1
  }

}
