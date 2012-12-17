package org.wquery.model

case class Arc(val relation: Relation, val from: String, val to: String) {
  override def toString = relation.name + "(" + from + "," + to + ")"

  def isCanonical = from == Relation.Source && to == Relation.Destination

  def isInverse = from == Relation.Destination && to == Relation.Source
}
