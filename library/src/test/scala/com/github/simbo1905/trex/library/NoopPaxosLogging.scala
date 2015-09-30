package com.github.simbo1905.trex.library

object NoopPaxosLogging extends PaxosLogging {
  override def info(msg: String): Unit = {}

  override def info(msg: String, one: Any): Unit = {}

  override def info(msg: String, one: Any, two: Any): Unit = {}

  override def info(msg: String, one: Any, two: Any, three: Any): Unit = {}

  override def info(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = {}

  override def debug(msg: String, one: Any, two: Any): Unit = {}

  override def debug(msg: String, one: Any, two: Any, three: Any): Unit = {}

  override def debug(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = {}

  override def error(msg: String): Unit = {}
}
