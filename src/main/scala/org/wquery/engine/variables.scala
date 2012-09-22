package org.wquery.engine
import scalaz._
import Scalaz._

sealed abstract class Variable(val name: String) {
  val isUnnamed = name == "_"
}

sealed abstract class PathVariable(override val name: String) extends Variable(name)

case class StepVariable(override val name: String) extends PathVariable(name) {
  override def toString = "$" + name
}

case class TupleVariable(override val name: String) extends PathVariable(name) {
  override def toString = "@" + name
}

case class SetVariable(override val name: String) extends Variable(name) {
  override def toString = "%" + name
}

object Variable {
  implicit val VariableEqual = equalA[Variable]
}

object StepVariable {
  val ContextVariable = StepVariable("__#")
  val Unnamed = StepVariable("_")
}

object TupleVariable {
  val Unnamed = TupleVariable("_")
}