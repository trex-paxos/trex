package com.github.trex_paxos.library

/**
 * We would like to log per agent rather than per class/trait so we pass the logger around.
 * This API happens to follow Akka logging
 */
trait PaxosLogging {
  def isErrorEnabled: Boolean = false

  def isInfoEnabled: Boolean = false

  def isDebugEnabled: Boolean = false

  def isWarningEnabled: Boolean = false

  def info(msg: String): Unit

  def info(msg: String, one: Any): Unit

  def info(msg: String, one: Any, two: Any): Unit

  def info(msg: String, one: Any, two: Any, three: Any): Unit

  def info(msg: String, one: Any, two: Any, three: Any, four: Any): Unit

  def debug(msg: String): Unit

  def debug(msg: String, one: Any): Unit

  def debug(msg: String, one: Any, two: Any): Unit

  def debug(msg: String, one: Any, two: Any, three: Any): Unit

  def debug(msg: String, one: Any, two: Any, three: Any, four: Any): Unit

  def error(msg: String): Unit

  def error(msg: String, one: Any): Unit

  def error(msg: String, one: Any, two: Any): Unit

  def error(msg: String, one: Any, two: Any, three: Any): Unit

  def error(msg: String, one: Any, two: Any, three: Any, four: Any): Unit

  def warning(msg: String): Unit

  def warning(msg: String, one: Any): Unit

  def warning(msg: String, one: Any, two: Any): Unit

  def warning(msg: String, one: Any, two: Any, three: Any): Unit

  def warning(msg: String, one: Any, two: Any, three: Any, four: Any): Unit

}
