package com.github.trex_paxos.library

// WARNING you should only return 0 if objects are equal else standard library red black trees will return random values on map lookups
object Ordering {

  implicit object IdentifierLogOrdering extends Ordering[Identifier] {
    def compare(o1: Identifier, o2: Identifier) = if (o1.logIndex == o2.logIndex) BallotNumberOrdering.compare(o1.number, o2.number) else if (o1.logIndex >= o2.logIndex) 1 else -1
  }

  implicit object BallotNumberOrdering extends Ordering[BallotNumber] {
    def compare(n1: BallotNumber, n2: BallotNumber) = if (n1 == n2) 0 else if (n1 > n2) 1 else -1
  }

}
