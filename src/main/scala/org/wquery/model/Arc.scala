package org.wquery.model

class Arc(val relation: Relation, val from: String, val to: String) {
  override def toString = relation.name + "(" + from + "," + to + ")"
  
  def isCanonical = from == Relation.Source && to == Relation.Destination
  
  def isInverse = from == Relation.Destination && to == Relation.Source
}

object Arc {    
  def apply(relation: Relation, from: String, to: String) = new Arc(relation, Relation.Source , Relation.Destination) 
    
  def apply(relation: Relation) = new Arc(relation, Relation.Source , Relation.Destination)    
    
  def inverse(relation: Relation) = new Arc(relation, Relation.Destination , Relation.Source)    
}