package org.wquery.model

import org.wquery.WQueryModelException
import collection.immutable.List
import scalaz._
import Scalaz._

class WordNetSchema(store: WordNetStore) {
  val maxPathSize = 20 // TODO estimate using WordNet content

  def relations = store.relations

  def getRelation(name: String, arguments: Map[String, Set[DataType]]) = {
    val extendedArguments = arguments.map{ case (name, types) => if (types.contains(StringType)) (name, types + POSType) else (name, types) }
    store.relations.find(r => r.name == name && extendedArguments.forall{ case (name, types) => r.getArgument(name).map(arg => types.contains(arg.nodeType)).getOrElse(false) })
  }

  def demandRelation(name: String, arguments: Map[String, Set[DataType]]) = {
    getRelation(name, arguments).getOrElse(throw new WQueryModelException("Relation '" + name + "' not found"))
  }

  def containsRelation(name: String, arguments: Map[String, Set[DataType]]) = getRelation(name, arguments).isDefined

  def extendMaxCount(pathCount: Option[BigInt], pattern: ArcPattern) = {
    pathCount.map(_ * 2) // TODO estimate using WordNet content
  }

  def fetchMaxCount(relation: Relation, from: List[(String, List[Any])], to: List[String]) = {
    from.map{ case (_, l) => if (l.isEmpty) 10000 else l.size }.sum // TODO estimate using WordNet content
  }

  def fringeMaxCount(relations: List[(Relation, String)]) = {
    10000 // TODO estimate using WordNet content
  }

  // extendCount

  // fetchCount
}
