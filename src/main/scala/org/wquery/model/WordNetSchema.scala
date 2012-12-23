package org.wquery.model

import org.wquery.WQueryModelException
import scalaz._
import Scalaz._

class WordNetSchema(wordNet: WordNet) {
  def relations = wordNet.relations

  def stats = wordNet.stats

  def getRelation(name: String, arguments: Map[String, Set[DataType]]) = {
    val extendedArguments = arguments.map{ case (name, types) => if (types.contains(StringType)) (name, types + POSType) else (name, types) }
    wordNet.relations.find(r => r.name == name &&
      extendedArguments.forall{ case (name, types) => r.getArgument(name).some(arg => types.contains(arg.nodeType)).none(false) })
  }

  def demandRelation(name: String, arguments: Map[String, Set[DataType]]) = {
    getRelation(name, arguments)|(throw new WQueryModelException("Relation '" + name + "' not found"))
  }

  def containsRelation(name: String, arguments: Map[String, Set[DataType]]) = getRelation(name, arguments).isDefined
}
