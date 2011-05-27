package org.wquery.emitter
import org.wquery.engine.Result

trait WQueryEmitter {
  def emit(result: Result):String
}
