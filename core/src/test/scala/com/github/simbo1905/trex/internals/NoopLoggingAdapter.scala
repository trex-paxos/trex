package com.github.simbo1905.trex.internals

import akka.event.LoggingAdapter

object NoopLoggingAdapter extends LoggingAdapter {
  override def isErrorEnabled: Boolean = false

  override protected def notifyInfo(message: String): Unit = {}

  override def isInfoEnabled: Boolean = false

  override def isDebugEnabled: Boolean = false

  override protected def notifyError(message: String): Unit = {}

  override protected def notifyError(cause: Throwable, message: String): Unit = {}

  override def isWarningEnabled: Boolean = false

  override protected def notifyWarning(message: String): Unit = {}

  override protected def notifyDebug(message: String): Unit = {}
}
