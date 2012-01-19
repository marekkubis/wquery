package org.wquery.engine
import scalaz._
import Scalaz._

sealed abstract class Variable(val name: String)

case class StepVariable(override val name: String) extends Variable(name) {
  override def toString = "$" + name
}

case class PathVariable(override val name: String) extends Variable(name) {
  override def toString = "@" + name
}

object Variable {
  implicit val VariableEqual = equalA[Variable]
}
