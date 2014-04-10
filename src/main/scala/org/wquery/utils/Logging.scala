package org.wquery.utils
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
