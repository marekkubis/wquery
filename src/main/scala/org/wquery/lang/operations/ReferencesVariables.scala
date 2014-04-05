package org.wquery.lang.operations

import org.wquery.lang.Variable

trait ReferencesVariables {
  val referencedVariables: Set[Variable]
}
