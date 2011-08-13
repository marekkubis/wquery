package org.wquery.model

import org.wquery.WQueryModelException

class WordNetSchema(store: WordNetStore) {
  def getRelation(name: String, sourceTypes: Set[DataType], sourceName: String) = {
    store.relations.find(r => r.name == name && r.arguments.get(sourceName).map(sourceTypes.contains(_)).getOrElse(false))
  }

  def demandRelation(name: String, sourceType: Set[DataType], sourceName: String) = {
    getRelation(name, sourceType, sourceName).getOrElse(throw new WQueryModelException("Relation '" + name + "' with source type " + sourceType + " not found"))
  }

  def containsRelation(name: String, sourceType: Set[DataType], sourceName: String) = getRelation(name, sourceType, sourceName) != None
}
