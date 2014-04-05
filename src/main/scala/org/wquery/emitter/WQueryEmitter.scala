package org.wquery.emitter
import org.wquery.lang.Result

trait WQueryEmitter {
  def emit(result: Result):String
}
