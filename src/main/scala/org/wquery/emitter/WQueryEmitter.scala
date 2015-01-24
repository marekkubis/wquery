package org.wquery.emitter

import org.wquery.WQueryEmitterNotFoundException
import org.wquery.lang.Result

import scala.collection.mutable


trait WQueryEmitter {
  def emit(result: Result):String
}

object WQueryEmitter {
  val emitters = mutable.Map[String, WQueryEmitter]()

  registerEmitter("tsv", new TsvWQueryEmitter())
  registerEmitter("raw", new RawWQueryEmitter())
  registerEmitter("plain", new PlainWQueryEmitter(escaping = false))
  registerEmitter("escaping", new PlainWQueryEmitter(escaping = true))

  def registerEmitter(name: String, emitter: WQueryEmitter) {
    emitters.put(name, emitter)
  }

  def unregisterEmitter(name: String) { emitters.remove(name) }

  def getEmitter(name: String): Option[WQueryEmitter] = emitters.get(name)

  def demandEmitter(name: String): WQueryEmitter = {
    emitters.getOrElse(name, throw new WQueryEmitterNotFoundException(name))
  }
}