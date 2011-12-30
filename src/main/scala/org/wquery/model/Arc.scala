package org.wquery.model

case class Arc(relation: Relation, from: String, to: String) {
  override def toString = relation.name + "(" + from + "," + to + ")"
  
  def isCanonical = from == Relation.Source && to == Relation.Destination
  
  def isInverse = from == Relation.Destination && to == Relation.Source
}
