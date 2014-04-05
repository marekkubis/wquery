package org.wquery.lang

import scalaz._
import Scalaz._

abstract class Variable(val name: String) {
  val isUnnamed = name == Variable.EmptyName
}

object Variable {
  val EmptyName = "_"

  implicit val VariableEqual = equalA[Variable]
}

