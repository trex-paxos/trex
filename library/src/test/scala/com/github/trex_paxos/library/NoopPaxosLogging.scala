package com.github.trex_paxos.library

class EmptyLogging extends PaxosLogging {
  override def info(msg: String): Unit = {}

  override def info(msg: String, one: Any): Unit = {}

  override def info(msg: String, one: Any, two: Any): Unit = {}

  override def info(msg: String, one: Any, two: Any, three: Any): Unit = {}

  override def info(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = {}

  override def debug(msg: String, one: Any, two: Any): Unit = {}

  override def debug(msg: String, one: Any, two: Any, three: Any): Unit = {}

  override def debug(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = {}

  override def error(msg: String): Unit = {}

  override def warning(msg: String): Unit = {}

  override def warning(msg: String, one: Any, two: Any): Unit = {}

  override def debug(msg: String, one: Any): Unit = {}

  override def warning(msg: String, one: Any): Unit = {}

  override def warning(msg: String, one: Any, two: Any, three: Any): Unit = {}

  override def warning(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = {}

  override def error(msg: String, one: Any): Unit = {}

  override def error(msg: String, one: Any, two: Any): Unit = {}

  override def error(msg: String, one: Any, two: Any, three: Any): Unit = {}

  override def error(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = {}
}

object NoopPaxosLogging extends EmptyLogging
