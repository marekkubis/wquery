package org.wquery.model

class WordNetStats(relations: List[Relation], val fetchAllMaxCounts: Map[(Relation, String), BigInt], val extendValueMaxCounts: Map[(Relation, String), BigInt]) {
  val maxPathSize = 20 // TODO estimate using WordNet content

  def fetchMaxCount(relation: Relation, from: List[(String, List[Any])], to: List[String]) = {
    (for ((argument, values) <- from) yield {
      if (values.isEmpty)
        fetchAllMaxCounts((relation, argument))
      else
        values.size * extendValueMaxCounts((relation, argument))
    }).min
  }

  def extendMaxCount(pathCount: Option[BigInt], pattern: ArcPattern) = {
    pathCount.map(_ * (
      for (relation <- pattern.relation.map(List(_)).getOrElse(relations);
        source <- if (pattern.source.isUnnamed) relation.argumentNames else List(pattern.source.name))
        yield extendValueMaxCounts(relation, pattern.source.name)).sum)
  }
}