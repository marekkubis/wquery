package org.wquery

import engine._
import loader.{LmfLoader, GridLoader, WordNetLoader}
import model.impl.InMemoryWordNet
import operations.{Bindings, BindingsSchema}
import org.wquery.model.WordNet
import org.wquery.parser.WParsers
import org.wquery.utils.Logging
import collection.mutable.ListBuffer

class WQuery(val wordNet: WordNet) extends Logging {
  val bindingsSchema = BindingsSchema()
  val bindings = Bindings()
  val parser = new Object with WParsers

  def execute(input: String): Result = {
    try {
      debug("Quer: " + input)

      val expr = parser parse input
      debug("Expr: " + expr)

      val plan = expr.evaluationPlan(wordNet.schema, bindingsSchema, Context())
      debug("Plan: " + plan)

      val dataSet = plan.evaluate(wordNet, bindings, Context())
      debug("Eval: " + dataSet.pathCount + " tuple(s) found")

      Answer(wordNet, dataSet)
    } catch {
      case e: WQueryException => Error(e)
    }
  }
}

object WQuery {
  val loaders = new ListBuffer[WordNetLoader]()

  registerLoader(new LmfLoader)
  registerLoader(new GridLoader)

  def registerLoader(loader: WordNetLoader) { loaders += loader }

  def unregisterLoader(loader: WordNetLoader) { loaders -= loader }

  def getInstance(from: String): WQuery = {
    createInstance(from: String) // TODO temporary stub
  }

  def createInstance(from: String, where: String = "memory"): WQuery = {
    val wordNet = where match {
      case _ =>
        new InMemoryWordNet
    }

    val validLoaders = loaders.filter(_.canLoad(from))

    if (validLoaders.isEmpty)
      throw new WQueryLoadingException(from + " cannot be loaded by any loader")

    validLoaders.head.load(from, wordNet)
    new WQuery(wordNet)
  }
}
