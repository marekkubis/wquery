package org.wquery.model
import org.wquery.WQueryModelException

case class Relation(name: String, arguments: Map[String, NodeType]) {
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
  // default argument names
  val Source = "source"
  val Destination = "destination"

  // property names
  val Transitivity = "transitivity"
  val Symmetry = "symmetry"
  val RequiredBy = "required_by"
  val Functional = "functional"
  val Properties = List(Transitivity, Symmetry, RequiredBy, Functional)

  // property actions
  val Restore = "restore"
  val Preserve = "preserve"
  val PropertyActions = List(Restore, Preserve)

  def unary(name: String, sourceType: NodeType) = {
    Relation(name, Map((Relation.Source, sourceType)))
  }

  def binary(name: String, sourceType: NodeType, destinationType: NodeType) = {
    Relation(name, Map((Relation.Source, sourceType), (Relation.Destination, destinationType)))
  }
}

sealed abstract class Symmetry

case object Symmetric extends Symmetry
case object Antisymmetric extends Symmetry
case object NonSymmetric extends Symmetry
