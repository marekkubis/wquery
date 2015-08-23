package org.wquery.lang

import org.wquery._
import org.wquery.lang.operations._
import org.wquery.lang.parsers.WLanguageParsers
import org.wquery.model.{DataSet, WordNet}
import org.wquery.path.operations.ConstantOp
import org.wquery.utils.Logging

class WLanguage(val wordNet: WordNet, parsers: WLanguageParsers) extends Logging {
  val bindingsSchema = BindingsSchema()
  val bindings = Bindings()

  def bindSetVariable(name: String, dataSet: DataSet) {
    bindingsSchema.bindSetVariableType(name, ConstantOp(dataSet), 0, true)
    bindings.bindSetVariable(name, dataSet)
  }

  def execute(input: String, makeDistinct: Boolean = false, sort: Boolean = false): Result = {
    try {
      debug("Quer: " + input)

      val expr = parsers parse input
      debug("Expr: " + expr)

      val op = expr.evaluationPlan(wordNet.schema, bindingsSchema, Context())
      val distinctOp = if (makeDistinct) FunctionOp(DistinctFunction, Some(op)) else op
      val sortedOp = if (sort) FunctionOp(SortFunction, Some(distinctOp)) else distinctOp
      debug("Plan: " + sortedOp)

      val dataSet = sortedOp.evaluate(wordNet, bindings, Context())
      debug("Eval: " + dataSet.pathCount + " tuple(s) found")

      Answer(wordNet, dataSet)
    } catch {
      case e: WQueryException => Error(e)
    }
  }
}

