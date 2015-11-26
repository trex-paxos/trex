package com.github.trex_paxos.library

import scala.collection.mutable.ArrayBuffer

/**
 * codacy.com complains about using var in tests or using '.get' on atomics so this is our own box for testing purposes.
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
