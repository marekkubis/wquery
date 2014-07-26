package org.wquery.loader

import java.io.InputStream

import org.wquery.WQueryLoaderNotFoundException
import org.wquery.model.WordNet

import scala.collection.mutable

trait WordNetLoader {
  def load(input: InputStream): WordNet
}

object WordNetLoader {
  val loaders = mutable.Map[String, WordNetLoader]()

  registerLoader("lmf", new LmfLoader)
  registerLoader("deb", new DebLoader)
  registerLoader("wn", new WnLoader)

  def registerLoader(name: String, loader: WordNetLoader) {
    loaders.put(name, loader)
  }

  def unregisterLoader(name: String) { loaders.remove(name) }

  def getLoader(name: String): Option[WordNetLoader] = loaders.get(name)

  def demandLoader(name: String): WordNetLoader = {
    loaders.getOrElse(name, throw new WQueryLoaderNotFoundException(name))
  }
}
