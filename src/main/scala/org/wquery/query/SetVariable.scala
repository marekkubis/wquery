package org.wquery.query

import org.wquery.lang.Variable

case class SetVariable(override val name: String) extends Variable(name) {
  override def toString = "%" + name
}

object SetVariable {
  val FunctionArgumentsVariable = "A"
}