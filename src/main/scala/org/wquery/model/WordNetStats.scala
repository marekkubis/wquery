package org.wquery.model

import scalaz._
import Scalaz._

class WordNetStats(relations: List[Relation], val fetchAllMaxCounts: Map[(Relation, String), BigInt],
                   val extendValueMaxCounts: Map[(Relation, String), BigInt]) {
  val maxPathSize = 20 // TODO estimate using WordNet content

  def domainSize = WordNet.domainRelations.map(r => fetchAllMaxCounts((r, Relation.Source))).sum

  def fetchMaxCount(relation: Relation, from: List[(String, List[Any])], to: List[String]) = {
    (for ((argument, values) <- from) yield {
      if (values.isEmpty)
        fetchAllMaxCounts((relation, argument))
      else
        values.size * extendValueMaxCounts((relation, argument))
    }).min
  }

  def extendMaxCount(pathCount: Option[BigInt], pattern: ArcPattern, direction: Direction) = {
    val sources = direction match {
      case Forward =>
        List(pattern.source.name)
      case Backward =>
        pattern.destinations.map(_.name)
    }

    val extendMaxCount = (for (relation <- pattern.relation.some(List(_)).none(relations.filter(_.isTraversable));
         source <- if (pattern.source.isUnnamed) relation.argumentNames else sources)
    yield extendValueMaxCounts(relation, source)).sum

    pathCount.map(_ * extendMaxCount)
  }
}
