package org.wquery.model.impl

import com.twitter.chill.ScalaKryoInstantiator

class InMemoryWordNetKryoInstantiator {
  val instantiator = new ScalaKryoInstantiator

  def newKryo() = {
    val kryo = instantiator.newKryo()
    kryo.register(classOf[InMemoryWordNetStore], new InMemoryWordNetStoreSerializer)
    kryo
  }
}
