package com.github.trex_paxos.library

import scala.collection.mutable.ArrayBuffer

/**
 * codacy.com complains about using var in tests or using '.get' on atomics so this is our own box mainly used for testing
 * purposes as it doesn't seem worth a new module nor a new dependency to avoid having to add this into the main codebase.
 */
class Box[T](value: Option[T]) {
  val v = ArrayBuffer[T]()
  value foreach {
    v += _
  }
  def apply() = v(0)
  def apply(value: T) = v.isEmpty match {
    case true => v += value
    case false => v.update(0, value)
  }
}

object Box {
  def apply[T](value: T): Box[T] = {
    return new Box[T](Option(value))
  }
}