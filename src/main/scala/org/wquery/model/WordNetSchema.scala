package org.wquery.model

import org.wquery.WQueryModelException
import scalaz._
import Scalaz._

class WordNetSchema(wordNet: WordNet) {
  def relations = wordNet.relations

  def stats = wordNet.stats

  def getRelation(name: String, arguments: Map[String, Set[DataType]], includingMeta: Boolean = false) = {
    val extendedArguments = arguments.map{ case (name, types) => if (types.contains(StringType)) (name, types + POSType) else (name, types) }
    val relations = wordNet.relations ++ (includingMeta ?? WordNet.Meta.relations)
    relations.find(r => r.name == name &&
      extendedArguments.forall{ case (name, types) => r.getArgument(name).some(arg => types.contains(arg.nodeType)).none(false) })
  }

  def demandRelation(name: String, arguments: Map[String, Set[DataType]], includingMeta: Boolean = false) = {
    getRelation(name, arguments, includingMeta)|(throw new WQueryModelException("Relation '" + name + "' not found"))
  }

  def containsRelation(name: String, arguments: Map[String, Set[DataType]], includingMeta: Boolean = false) = {
    getRelation(name, arguments, includingMeta).isDefined
  }
}
