package org.wquery.compile

import java.io.OutputStream

import com.esotericsoftware.kryo.io.Output
import org.wquery.model.WordNet
import org.wquery.model.impl.InMemoryWordNetKryoInstantiator

class WCompile {
  def compile(wordNet: WordNet, ostream: OutputStream) {
    val instantiator = new InMemoryWordNetKryoInstantiator
    val kryo = instantiator.newKryo()
    val output = new Output(ostream)

    kryo.writeObject(output, wordNet)
    output.close()
  }
}
