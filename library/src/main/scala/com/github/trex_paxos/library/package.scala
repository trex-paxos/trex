package com.github.trex_paxos

package object library {

  /** http://stackoverflow.com/a/5597750/329496 */
  case class Lens[A, B](get: A => B, set: (A, B) => A) extends ((A) => B) with Immutable {
    def apply(whole: A): B = get(whole)

    def mod(a: A, f: B => B) = set(a, f(this (a)))

    def compose[C](that: Lens[C, A]) = Lens[C, B](
      c => this(that(c)),
      (c, b) => that.mod(c, set(_, b))
    )

    def andThen[C](that: Lens[B, C]) = that compose this
  }

  object HighestCommittedIndex {
    def unapply(data: PaxosData) = Some(data.progress.highestCommitted.logIndex)
  }

}
