package com.github.trex_paxos.core

import akka.actor.ActorLogging
import com.github.trex_paxos.library.PaxosLogging

trait AkkaLoggingAdapter extends PaxosLogging {
  this: ActorLogging =>

  override def info(msg: String): Unit = log.info(msg)

  override def debug(msg: String): Unit = log.debug(msg)

  override def info(msg: String, one: Any): Unit = log.info(msg, one)

  override def info(msg: String, one: Any, two: Any): Unit = log.info(msg, one, two)

  override def info(msg: String, one: Any, two: Any, three: Any): Unit = log.info(msg, one, two, three)

  override def info(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = log.info(msg, one, two, three, four)

  override def info(msg: String, one: Any, two: Any, three: Any, four: Any, five: Any): Unit = log.info(msg, one, two, three, four + "|" + five)

  override def debug(msg: String, one: Any): Unit = log.debug(msg, one)

  override def debug(msg: String, one: Any, two: Any): Unit = log.debug(msg, one, two)

  override def debug(msg: String, one: Any, two: Any, three: Any): Unit = log.debug(msg, one, two, three)

  override def debug(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = log.debug(msg, one, two, three, four)

  override def debug(msg: String, one: Any, two: Any, three: Any, four: Any, five: Any): Unit = log.debug(msg, one, two, three, four + "|" + five)

  override def error(msg: String): Unit = log.error(msg)

  override def error(msg: String, one: Any): Unit = log.error(msg, one)

  override def error(msg: String, one: Any, two: Any): Unit = log.error(msg, one, two)

  override def error(msg: String, one: Any, two: Any, three: Any): Unit = log.error(msg, one, two, three)

  override def error(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = log.error(msg, one, two, three, four)

  override def error(msg: String, one: Any, two: Any, three: Any, four: Any, five: Any): Unit = log.error(msg, one, two, three,four + "|" + five)

  override def warning(msg: String): Unit = log.warning(msg)

  override def warning(msg: String, one: Any): Unit = log.warning(msg, one)

  override def warning(msg: String, one: Any, two: Any): Unit = log.warning(msg, one, two)

  override def warning(msg: String, one: Any, two: Any, three: Any): Unit = log.warning(msg, one, two, three)

  override def warning(msg: String, one: Any, two: Any, three: Any, four: Any): Unit = log.warning(msg, one, two, three, four)

  override def warning(msg: String, one: Any, two: Any, three: Any, four: Any, five: Any): Unit = log.warning(msg, one, two, three, four + "|" + five)
}
