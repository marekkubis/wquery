package org.wquery.model

import org.wquery.WQueryModelException

class WordNetSchema(store: WordNetStore) {
  def relations = store.relations

  def getRelation(name: String, arguments: Map[String, Set[DataType]]) = {
    val extendedArguments = arguments.map { case (name, types) => if (types.contains(StringType)) (name, types + POSType) else (name, types) }
    store.relations.find(r => r.name == name && extendedArguments.forall{ case (name, types) => r.arguments.get(name).map(types.contains(_)).getOrElse(false) })
  }

  def demandRelation(name: String, arguments: Map[String, Set[DataType]]) = {
    getRelation(name, arguments).getOrElse(throw new WQueryModelException("Relation '" + name + "' not found"))
  }

  def containsRelation(name: String, arguments: Map[String, Set[DataType]]) = getRelation(name, arguments) != None
}
