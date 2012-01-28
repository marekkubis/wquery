package org.wquery.engine
import scalaz._
import Scalaz._

sealed abstract class Variable(val name: String) {
  val isUnnamed = name == "_"
}

case class StepVariable(override val name: String) extends Variable(name) {
  override def toString = "$" + name
}

case class PathVariable(override val name: String) extends Variable(name) {
  override def toString = "@" + name
}

object Variable {
  implicit val VariableEqual = equalA[Variable]
}

object StepVariable {
  val ContextVariable = StepVariable("__#")
}