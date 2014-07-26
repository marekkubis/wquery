package org.wquery.utils

import ch.qos.logback.classic.{Level, Logger}
import org.slf4j.LoggerFactory

trait Logging {
  val log = LoggerFactory.getLogger(getClass)

  def debug(message: => String) { 
    if (log.isDebugEnabled)
      log.debug(message) 
  }

  def trace(message: => String) { 
    if (log.isTraceEnabled) log.trace(message) 
  }

  def info(message: => String) { 
    if (log.isInfoEnabled) 
      log.info(message) 
  }

  def warn(message: => String) { 
    if (log.isWarnEnabled)
      log.warn(message) 
  }

  def error(message: => String) { 
    if (log.isErrorEnabled)
      log.error(message) 
  }

  def debug(message: => String, t: Throwable) { 
    if (log.isDebugEnabled)
      log.debug(message, t) 
  }

  def trace(message: => String, t: Throwable) { 
    if (log.isTraceEnabled)
      log.trace(message, t) 
  }

  def info(message: => String, t: Throwable) { 
    if (log.isInfoEnabled) 
      log.info(message, t) 
  }

  def warn(message: => String, t: Throwable) { 
    if (log.isWarnEnabled)
      log.warn(message, t) 
  }

  def error(message: => String, t: Throwable) { 
    if (log.isErrorEnabled)
      log.error(message, t) 
  }

}

object Logging {
  def tryDisableLoggers() {
    // slf4j - wquery and akka
    LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME) match {
      case logger: Logger =>
        logger.setLevel(Level.OFF)
      case logger =>
        logger.warn("Cannot disable the root logger (the logger is not an instance of " + classOf[Logger].getName + ")")
    }

    // java.util.Logging
    java.util.logging.Logger.getLogger("").setLevel(java.util.logging.Level.OFF)
  }
}