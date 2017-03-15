package com.github.trex_paxos.demo

import com.github.trex_paxos.library.PaxosLogging

import org.apache.logging.log4j.LogManager

object Log4jPaxosLogging {
  val warning = "Don't forget to set system property -DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector to silence this warning and make all Log4jPaxosLogging loggers asynchronous. "

  val log4j = LogManager.getLogger(classOf[PaxosLogging])

  Option(System.getProperty("Log4jContextSelector")) match {
    case None =>
      log4j.warn(warning)
    case _ => // good
  }
}

trait Log4jPaxosLogging extends PaxosLogging {

  import Log4jPaxosLogging.log4j

  override def info(msg: String): Unit = log4j.info(msg)

  override def info(msg: String, one: Any): Unit = log4j.info(msg, one)

  override def info(msg: String, one: Any, two: Any): Unit = log4j.info(msg, one, two)

  override def info(msg: String, one: Any, two: Any, three: Any): Unit = log4j.info(msg, one, two, three)

  override def info(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = log4j.info(msg, one, two, three, four)

  override def info(msg: String, one: Any, two: Any, three: Any, four: Any, five: Any): Unit = log4j.info(msg, one, two, three, four, five)

  override def debug(msg: String): Unit = log4j.debug(msg)

  override def debug(msg: String, one: Any): Unit = log4j.debug(msg, one)

  override def debug(msg: String, one: Any, two: Any): Unit = log4j.debug(msg, one, two)

  override def debug(msg: String, one: Any, two: Any, three: Any): Unit = log4j.debug(msg, one, two, three)

  override def debug(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = log4j.debug(msg, one, two, three, four)

  override def debug(msg: String, one: Any, two: Any, three: Any, four: Any, five: Any): Unit = log4j.debug(msg, one, two, three, four, five)

  override def error(msg: String): Unit = log4j.error(msg)

  override def error(msg: String, one: Any): Unit = log4j.error(msg, one)

  override def error(msg: String, one: Any, two: Any): Unit = log4j.error(msg, one, two)

  override def error(msg: String, one: Any, two: Any, three: Any): Unit = log4j.error(msg, one, two, three)

  override def error(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = log4j.error(msg, one, two, three, four)

  override def error(msg: String, one: Any, two: Any, three: Any, four: Any, five: Any): Unit = log4j.error(msg, one, two, three, four, five)

  override def warning(msg: String): Unit = log4j.warn(msg)

  override def warning(msg: String, one: Any): Unit = log4j.warn(msg, one)

  override def warning(msg: String, one: Any, two: Any): Unit = log4j.warn(msg, one, two)

  override def warning(msg: String, one: Any, two: Any, three: Any): Unit = log4j.warn(msg, one, two, three)

  override def warning(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = log4j.warn(msg, one, two, three, four)

  override def warning(msg: String, one: Any, two: Any, three: Any, four: Any, five: Any): Unit = log4j.warn(msg, one, two, three, four, five)
}
