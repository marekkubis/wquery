package org.wquery.model

class Relation(val name: String, val arguments: Map[String, NodeType]) {

  def sourceType = arguments(Relation.Source) 
  
  def destinationType = arguments(Relation.Destination)

  if (!arguments.contains(Relation.Source))
    throw new IllegalArgumentException("An attempt to create Relation '" + name + "' without source argument")
  
  if (!arguments.contains(Relation.Destination))
    throw new IllegalArgumentException("An attempt to create Relation '" + name + "' without destination argument")
}

object Relation {
  val Source = "source" // default argument name
  val Destination = "destination" // default argument name

  def apply(name: String, sourceType: NodeType, destinationType: NodeType): Relation
    = apply(name, sourceType, destinationType, Map())     
  
  def apply(name: String, sourceType: NodeType, destinationType: NodeType, arguments: Map[String, NodeType]): Relation
    = new Relation(name, arguments + Pair(Source, sourceType) + Pair(Destination, destinationType))  
}
