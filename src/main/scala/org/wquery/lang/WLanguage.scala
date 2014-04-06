package org.wquery.lang

import org.wquery.model.WordNet
import org.wquery._
import org.wquery.lang._
import org.wquery.lang.operations._
import org.wquery.lang.parsers.WParsers
import org.wquery.utils.Logging

class WLanguage(val wordNet: WordNet, parsers: WParsers) extends Logging {
  val bindingsSchema = BindingsSchema()
  val bindings = Bindings()

  def execute(input: String, makeDistinct: Boolean = false, sort: Boolean = false): Result = {
    try {
      debug("Quer: " + input)

      val expr = parsers parse input
      debug("Expr: " + expr)

      val op = expr.evaluationPlan(wordNet.schema, bindingsSchema, Context())
      val distinctOp = if (makeDistinct) FunctionOp(DistinctFunction, op) else op
      val sortedOp = if (sort) FunctionOp(SortFunction, distinctOp) else distinctOp
      debug("Plan: " + sortedOp)

      val dataSet = sortedOp.evaluate(wordNet, bindings, Context())
      debug("Eval: " + dataSet.pathCount + " tuple(s) found")

      Answer(wordNet, dataSet)
    } catch {
      case e: WQueryException => Error(e)
    }
  }
}

