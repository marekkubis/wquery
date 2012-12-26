package org.wquery.model

case class Arc(val relation: Relation, val from: String, val to: String) {
  override def toString = relation.name + "(" + from + "," + to + ")"

  def isCanonical = from == Relation.Src && to == Relation.Dst

  def isInverse = from == Relation.Dst && to == Relation.Src
}
