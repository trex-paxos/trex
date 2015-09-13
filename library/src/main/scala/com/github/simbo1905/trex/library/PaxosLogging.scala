package com.github.simbo1905.trex.library;

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

   def debug(msg: String, one: Any, two: Any): Unit

   def debug(msg: String, one: Any, two: Any, three: Any): Unit

   def debug(msg: String, one: Any, two: Any, three: Any, four: Any): Unit

   def error(msg: String): Unit

 }
