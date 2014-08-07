package org.wquery.model.impl

import com.twitter.chill._
import org.wquery.model.Relation

import scala.collection.mutable.{Map => MMap}
import scala.concurrent.stm._

class InMemoryWordNetStore(val successors: TMap[Relation, TMap[(String, Any), Vector[Map[String, Any]]]])

class InMemoryWordNetStoreSerializer extends KSerializer[InMemoryWordNetStore] {
  // NOTE: TMaps are serialized using an approach similar to the one used in
  // the chill framework's TraversableSerializer for other types of collections

  def write(kryo: Kryo, output: Output, store: InMemoryWordNetStore) = atomic { implicit txn =>
    output.writeInt(store.successors.size, true)
    store.successors.foreach { succ =>
      kryo.writeClassAndObject(output, succ.asInstanceOf[AnyRef])
      output.flush()
    }
  }

  def read(kryo: Kryo, input: Input, c: Class[InMemoryWordNetStore]): InMemoryWordNetStore = atomic { implicit txn =>
    val successors = TMap[Relation, TMap[(String, Any), Vector[Map[String, Any]]]]()
    val size = input.readInt(true)
    var i = 0

    while (i < size) {
      val (k, v) = kryo.readClassAndObject(input).asInstanceOf[(Relation, MMap[(String, Any), scala.Vector[Map[String, Any]]])]
      successors(k) = TMap[(String, Any), Vector[Map[String, Any]]](v.toSeq:_*)
      i += 1
    }

    new InMemoryWordNetStore(successors)
  }
}
