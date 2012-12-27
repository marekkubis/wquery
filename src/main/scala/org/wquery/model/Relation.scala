package org.wquery.model
import org.wquery.WQueryModelException
import scalaz._
import Scalaz._

case class Relation(name: String, arguments: List[Argument]) {
  private val argumentsByName = arguments.map(arg => (arg.name, arg)).toMap

  val sourceType = arguments.head.nodeType

  val destinationType = argumentsByName.get(Relation.Dst).map(_.nodeType)

  val argumentNames = arguments.map(_.name)

  def isTraversable = arguments.size > 1

  def demandArgument(argument: String) = {
    argumentsByName.getOrElse(argument, throw new WQueryModelException("Relation '" + name + "' does not have argument '" + argument + "'"))
  }

  def getArgument(argument: String) = argument match {
    case Relation.Src =>
      arguments.headOption
    case Relation.Dst =>
      arguments.tail.headOption
    case _ =>
      argumentsByName.get(argument)
  }

  override def toString = name
}

case class Argument(val name: String, val nodeType: NodeType)

object Relation {
  val AnyName = "_"

  // default argument names
  val Src = "src"
  val Dst = "dst"

  // property names
  val Transitivity = "transitivity"
  val Symmetry = "symmetry"
  val RequiredBy = "required_by"
  val Functional = "functional"

  // property actions
  val Restore = "restore"
  val Preserve = "preserve"

  def unary(name: String, sourceType: NodeType) = {
    Relation(name, List(Argument(Relation.Src, sourceType)))
  }

  def binary(name: String, sourceType: NodeType, destinationType: NodeType) = {
    Relation(name, List(Argument(Relation.Src, sourceType), Argument(Relation.Dst, destinationType)))
  }
}

sealed abstract class Symmetry

case object Symmetric extends Symmetry
case object Antisymmetric extends Symmetry
case object NonSymmetric extends Symmetry