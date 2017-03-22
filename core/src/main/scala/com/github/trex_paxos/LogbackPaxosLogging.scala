package com.github.trex_paxos

import com.github.trex_paxos.library.PaxosLogging
import org.slf4j.LoggerFactory

/**
  * You should use AsyncAppender for performance so this class asks for a specific appender "paxosasync"
  */
trait LogbackPaxosLogging extends PaxosLogging {

  val logbak = LoggerFactory.getLogger("paxosasync")

  override def info(msg: String): Unit = logbak.info(msg)

  override def info(msg: String, one: Any): Unit = logbak.info(msg, one)

  override def info(msg: String, one: Any, two: Any): Unit = logbak.info(msg, one, two)

  override def info(msg: String, one: Any, two: Any, three: Any): Unit = logbak.info(msg, Array[Any](one, two, three))

  override def info(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = logbak.info(msg, Array[Any](one, two, three, four))

  override def info(msg: String, one: Any, two: Any, three: Any, four: Any, five: Any): Unit = logbak.info(msg, Array[Any](one, two, three, four, five))

  override def debug(msg: String): Unit = logbak.debug(msg)

  override def debug(msg: String, one: Any): Unit = logbak.debug(msg, one)

  override def debug(msg: String, one: Any, two: Any): Unit = logbak.debug(msg, one, two)

  override def debug(msg: String, one: Any, two: Any, three: Any): Unit = logbak.debug(msg, Array[Any](one, two, three))

  override def debug(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = logbak.debug(msg, Array[Any](one, two, three, four))

  override def debug(msg: String, one: Any, two: Any, three: Any, four: Any, five: Any): Unit = logbak.debug(msg, Array[Any](one, two, three, four, five))

  override def error(msg: String): Unit = logbak.error(msg)

  override def error(msg: String, one: Any): Unit = logbak.error(msg, one)

  override def error(msg: String, one: Any, two: Any): Unit = logbak.error(msg, one, two)

  override def error(msg: String, one: Any, two: Any, three: Any): Unit = logbak.error(msg, Array[Any](one, two, three))

  override def error(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = logbak.error(msg, Array[Any](one, two, three, four))

  override def error(msg: String, one: Any, two: Any, three: Any, four: Any, five: Any): Unit = logbak.error(msg, Array[Any](one, two, three, four, five))

  override def warning(msg: String): Unit = logbak.warn(msg)

  override def warning(msg: String, one: Any): Unit = logbak.warn(msg, one)

  override def warning(msg: String, one: Any, two: Any): Unit = logbak.warn(msg, one, two)

  override def warning(msg: String, one: Any, two: Any, three: Any): Unit = logbak.warn(msg, Array[Any](one, two, three))

  override def warning(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = logbak.warn(msg, Array[Any](one, two, three, four))

  override def warning(msg: String, one: Any, two: Any, three: Any, four: Any, five: Any): Unit = logbak.warn(msg, Array[Any](one, two, three, four, five))
}
