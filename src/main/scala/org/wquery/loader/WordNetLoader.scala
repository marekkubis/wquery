package org.wquery.loader

import org.wquery.WQueryLoadingException
import org.wquery.model.WordNet

import scala.collection.mutable.ListBuffer

trait WordNetLoader {
  def canLoad(url:String): Boolean

  def load(url:String): WordNet
}

object WordNetLoader {
  val loaders = new ListBuffer[WordNetLoader]()

  registerLoader(new LmfLoader)
  registerLoader(new GridLoader)
  registerLoader(new WnLoader)

  def registerLoader(loader: WordNetLoader) { loaders += loader }

  def unregisterLoader(loader: WordNetLoader) { loaders -= loader }

  def load(from: String): WordNet = {
    val validLoaders = loaders.filter(_.canLoad(from))

    if (validLoaders.isEmpty)
      throw new WQueryLoadingException(from + " cannot be loaded by any loader")

    validLoaders.head.load(from)
  }
}
