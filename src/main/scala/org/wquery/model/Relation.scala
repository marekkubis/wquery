package org.wquery.model
import org.wquery.WQueryModelException
import scalaz._
import Scalaz._

case class Relation(name: String, arguments: Set[Argument]) {
  private val argumentsByName = arguments.map(arg => (arg.name, arg)).toMap

  val sourceType = argumentsByName(Relation.Source).nodeType

  val destinationType = argumentsByName.get(Relation.Destination).map(_.nodeType)

  val argumentNames = Relation.Source :: (arguments.map(_.name) - Relation.Source - Relation.Destination)
    .toList.sortWith((x, y) => x < y) ++ argumentsByName.get(Relation.Destination).toList.map(_.name)

  def isTraversable = arguments.size > 1

  def demandArgument(argument: String) = {
    argumentsByName.getOrElse(argument, throw new WQueryModelException("Relation '" + name + "' does not have argument '" + argument + "'"))
  }

  def getArgument(argument: String) = argumentsByName.get(argument)

  override def toString = name

  if (!argumentsByName.contains(Relation.Source))
    throw new IllegalArgumentException("An attempt to create Relation '" + name + "' without source argument")
}

case class Argument(name: String, nodeType: NodeType)

object Relation {
  val AnyName = "_"

  // default argument names
  val Source = "src"
  val Destination = "dst"

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
    Relation(name, Set(Argument(Relation.Source, sourceType)))
  }

  def binary(name: String, sourceType: NodeType, destinationType: NodeType) = {
    Relation(name, Set(Argument(Relation.Source, sourceType), Argument(Relation.Destination, destinationType)))
  }
}

sealed abstract class Symmetry

case object Symmetric extends Symmetry
case object Antisymmetric extends Symmetry
case object NonSymmetric extends Symmetry
