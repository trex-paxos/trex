package com.github.simbo1905.trex.internals

import akka.actor.ActorLogging
import com.github.simbo1905.trex.library.PaxosLogging

trait AkkaLoggingAdapter extends PaxosLogging {
  this: ActorLogging =>

  override def error(msg: String): Unit = log.error(msg)

  override def debug(msg: String, one: Any, two: Any): Unit = log.debug(msg, one, two)

  override def debug(msg: String, one: Any, two: Any, three: Any): Unit = log.debug(msg, one, two, three)

  override def debug(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = log.debug(msg, one, two, three, four)

  override def info(msg: String): Unit = log.info(msg)

  override def info(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = log.info(msg, one, two, three, four)

  override def info(msg: String, one: Any, two: Any): Unit = log.info(msg, one, two)

  override def info(msg: String, one: Any): Unit = log.info(msg, one)

  override def info(msg: String, one: Any, two: Any, three: Any): Unit = log.info(msg, one, two, three)

  override def warning(msg: String): Unit = log.warning(msg)

  override def warning(msg: String, one: Any, two: Any): Unit = log.warning(msg, one, two)
}
