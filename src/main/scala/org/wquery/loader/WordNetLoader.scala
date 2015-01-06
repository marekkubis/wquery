package org.wquery.loader

import java.io.{FileInputStream, InputStream}

import org.wquery.model.WordNet
import org.wquery.{WQueryLoaderNotFoundException, WQueryStreamLoaderNotFoundException}

import scala.collection.mutable

trait WordNetLoader {
  def load(path: String): WordNet
}

trait StreamWordNetLoader extends WordNetLoader {
  def load(input: InputStream): WordNet

  override def load(path: String) = load(new FileInputStream(path))
}

object WordNetLoader {
  val loaders = mutable.Map[String, WordNetLoader]()

  registerLoader("lmf", new LmfLoader)
  registerLoader("deb", new DebLoader)
  registerLoader("pwn", new PWNLoader)
  registerLoader("wn", new WnLoader)

  def registerLoader(name: String, loader: WordNetLoader) {
    loaders.put(name, loader)
  }

  def unregisterLoader(name: String) { loaders.remove(name) }

  def getLoader(name: String): Option[WordNetLoader] = loaders.get(name)

  def demandLoader(name: String): WordNetLoader = {
    loaders.getOrElse(name, throw new WQueryLoaderNotFoundException(name))
  }

  def getStreamWordNetLoader(name: String): Option[StreamWordNetLoader] = {
    loaders.get(name).filter(_.isInstanceOf[StreamWordNetLoader])
      .map(_.asInstanceOf[StreamWordNetLoader])
  }

  def demandStreamWordNetLoader(name: String): StreamWordNetLoader = {
    val loader = demandLoader(name)

    loader match {
      case loader: StreamWordNetLoader =>
        loader
      case _ =>
        throw new WQueryStreamLoaderNotFoundException(name)
    }
  }
}
