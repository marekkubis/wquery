package org.wquery.printer

import java.io.OutputStream

import org.wquery.WQueryLoaderNotFoundException
import org.wquery.model.WordNet

import scala.collection.mutable

trait WordNetPrinter {
  def print(wordNet: WordNet, output: OutputStream)
}

object WordNetPrinter {
  val printers = mutable.Map[String, WordNetPrinter]()

  registerPrinter("deb", new DebPrinter)
  registerPrinter("wn", new WnPrinter)

  def registerPrinter(name: String, loader: WordNetPrinter) {
    printers.put(name, loader)
  }

  def unregisterPrinter(name: String) { printers.remove(name) }

  def getPrinter(name: String): Option[WordNetPrinter] = printers.get(name)

  def demandPrinter(name: String): WordNetPrinter = {
    printers.getOrElse(name, throw new WQueryLoaderNotFoundException(name))
  }
}