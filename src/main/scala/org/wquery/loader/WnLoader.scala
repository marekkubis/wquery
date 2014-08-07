package org.wquery.loader

import java.io.InputStream

import com.twitter.chill.Input
import org.wquery.model.impl.{InMemoryWordNet, InMemoryWordNetKryoInstantiator}
import org.wquery.utils.Logging

class WnLoader extends WordNetLoader with Logging {
  override def load(input: InputStream) = {
    val instantiator = new InMemoryWordNetKryoInstantiator
    val kryo = instantiator.newKryo()
    val wordNet = kryo.readObject(new Input(input), classOf[InMemoryWordNet])

    wordNet
  }
}
