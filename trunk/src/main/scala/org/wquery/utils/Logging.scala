package org.wquery.utils
import org.slf4j.LoggerFactory

trait Logging {
  val log = LoggerFactory.getLogger(getClass)
  
  def debug(message: => String) { if (log.isDebugEnabled) log.debug(message) }
  def trace(message: => String) { if (log.isTraceEnabled) log.trace(message) } 
  def info(message: => String) = { if (log.isInfoEnabled) log.info(message) }
  def warn(message: => String) = { if (log.isWarnEnabled) log.warn(message) }
  def error(message: => String) = { if (log.isErrorEnabled) log.error(message) }
}
