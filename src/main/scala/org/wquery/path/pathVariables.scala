package org.wquery.path

import scalaz._
import Scalaz._
import org.wquery.lang.Variable

sealed abstract class PathVariable(override val name: String) extends Variable(name)

case class StepVariable(override val name: String) extends PathVariable(name) {
  override def toString = "$" + name
}

case class TupleVariable(override val name: String) extends PathVariable(name) {
  override def toString = "@" + name
}

object StepVariable {
  val ContextVariable = StepVariable("__#")
  val Unnamed = StepVariable(Variable.EmptyName)
}

object TupleVariable {
  val Unnamed = TupleVariable(Variable.EmptyName)
}
