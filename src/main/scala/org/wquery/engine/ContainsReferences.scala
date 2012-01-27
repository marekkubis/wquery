package org.wquery.engine

trait ContainsReferences {
  val referencedVariables: Set[Variable]
  val referencesContext: Boolean
}