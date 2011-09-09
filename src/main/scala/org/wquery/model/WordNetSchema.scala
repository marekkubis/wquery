package org.wquery.model

import org.wquery.WQueryModelException

class WordNetSchema(store: WordNetStore) {
  def getRelation(name: String, arguments: Map[String, Set[DataType]]) = {
    store.relations.find(r => r.name == name && arguments.forall{ case (name, types) => r.arguments.get(name).map(types.contains(_)).getOrElse(false) })
  }

  def demandRelation(name: String, arguments: Map[String, Set[DataType]]) = {
    getRelation(name, arguments).getOrElse(throw new WQueryModelException("Relation '" + name + "' not found"))
  }

  def containsRelation(name: String, arguments: Map[String, Set[DataType]]) = getRelation(name, arguments) != None
}
