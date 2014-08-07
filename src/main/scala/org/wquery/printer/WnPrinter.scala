package org.wquery.printer

import java.io.OutputStream

import com.esotericsoftware.kryo.io.Output
import org.wquery.model.WordNet
import org.wquery.model.impl.InMemoryWordNetKryoInstantiator

class WnPrinter extends WordNetPrinter {
  def print(wordNet: WordNet, output: OutputStream) {
    val instantiator = new InMemoryWordNetKryoInstantiator
    val kryo = instantiator.newKryo()
    val out = new Output(output)

    kryo.writeObject(out, wordNet)
    out.close()
  }
}
