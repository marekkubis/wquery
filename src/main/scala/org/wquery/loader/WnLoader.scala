package org.wquery.loader

import java.io.FileInputStream

import com.twitter.chill.{Input, ScalaKryoInstantiator}
import org.wquery.model.impl.InMemoryWordNet
import org.wquery.utils.Logging

class WnLoader extends WordNetLoader with Logging {
  override def canLoad(url: String): Boolean = url.endsWith(".wn") // TODO provide a better check

  override def load(url: String) = {
    val instantiator = new ScalaKryoInstantiator
    val kryo = instantiator.newKryo()
    val fin = new FileInputStream(url)
    val input = new Input(fin)
    val wordNet = kryo.readObject(input, classOf[InMemoryWordNet])

    input.close()
    wordNet
  }
}
