// scalastyle:off number.of.types

package org.wquery.engine

import scalaz._
import Scalaz._
import org.wquery.model._
import org.wquery.engine.operations._
import org.wquery.update.operations._
import org.wquery.{FoundReferenceToUnknownVariableWhileCheckingException, WQueryStaticCheckException, WQueryEvaluationException}
import collection.mutable.ListBuffer

abstract class Expr

abstract class EvaluableExpr extends Expr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context): AlgebraOp
}

