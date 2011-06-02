package org.wquery.model
import org.wquery.WQueryModelException

case class Relation(val name: String, val arguments: Map[String, NodeType]) {
  val sourceType = arguments(Relation.Source) 
  
  val destinationType = arguments.get(Relation.Destination)
  
  val argumentNames = Relation.Source :: (arguments - Relation.Source - Relation.Destination).keys.toList.sortWith((x, y) => x < y) ++ List(Relation.Destination)
  
  def demandArgument(argument: String) = {
    arguments.getOrElse(argument, throw new WQueryModelException("Relation '" + name + "' does not have argument '" + argument + "'"))      
  }
  
  override def toString = name

  if (!arguments.contains(Relation.Source))
    throw new IllegalArgumentException("An attempt to create Relation '" + name + "' without source argument")
}

object Relation {
  val Source = "source" // default argument name
  val Destination = "destination" // default argument name

  def apply(name: String, sourceType: NodeType) = new Relation(name, Map((Source, sourceType)))

  def apply(name: String, sourceType: NodeType, destinationType: NodeType): Relation
    = apply(name, sourceType, destinationType, Map())     
  
  def apply(name: String, sourceType: NodeType, destinationType: NodeType, arguments: Map[String, NodeType]): Relation
    = new Relation(name, arguments + Pair(Source, sourceType) + Pair(Destination, destinationType))  
}
