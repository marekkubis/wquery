package org.wquery.model

case class Arc(relation: Relation, from: String, to: String) {
  override def toString = relation.name + "(" + from + "," + to + ")"

  def isCanonical = from == Relation.Src && to == Relation.Dst

  def isInverse = from == Relation.Dst && to == Relation.Src
}
