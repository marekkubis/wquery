package org.wquery.compile

import java.io.OutputStream

import com.esotericsoftware.kryo.io.Output
import com.twitter.chill.ScalaKryoInstantiator
import org.wquery.model.WordNet

class WCompile {
  def compile(wordNet: WordNet, ostream: OutputStream) {
    val instantiator = new ScalaKryoInstantiator
    // TODO instantiator.setRegistrationRequired(false)

    val kryo = instantiator.newKryo()
    val output = new Output(ostream)

    kryo.writeObject(output, wordNet)
    output.close()
  }
}
