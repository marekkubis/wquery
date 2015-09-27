package org.wquery.lang

import java.io.{File, FileReader}

import org.wquery._
import org.wquery.lang.operations._
import org.wquery.lang.parsers.WLanguageParsers
import org.wquery.model.{DataSet, WordNet}
import org.wquery.path.operations.ConstantOp
import org.wquery.reader.{ExpressionReader, InputLineReader}
import org.wquery.utils.Logging

import scala.collection.mutable.ListBuffer

class WLanguage(val wordNet: WordNet, parsers: WLanguageParsers, libraries: List[String]) extends Logging {
  val bindingsSchema = BindingsSchema()
  val bindings = Bindings()

  loadLibraries()

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

  def executeFile(file: File, failOnError: Boolean = true): List[Result] = {
    val expressionReader = new ExpressionReader(new InputLineReader(new FileReader(file)))
    val resultBuffer = new ListBuffer[Result]

    expressionReader.foreach { expr =>
      val result = execute(expr)
      resultBuffer.append(result)

      if (failOnError && result.isInstanceOf[Error]) {
        expressionReader.close()
        return resultBuffer.toList
      }
    }

    expressionReader.close()
    resultBuffer.toList
  }

  private def loadLibraries() {
    val libraryDir = System.getProperty("wquery.runtime.library") match {
      case null =>
        new File(this.getClass.getProtectionDomain.getCodeSource.getLocation.toURI).getParentFile
      case path =>
        new File(path)
    }

    for (library <- libraries) {
      for (file <- libraryDir.listFiles()) {
        if (file.getName.endsWith(library)) {
          val results = executeFile(file)

          if (results.nonEmpty) {
            results.last match {
              case Error(e) =>
                throw new LibraryNotLoadedException(file.getName, e)
              case _ =>
                /* do nothing */
            }
          }
        }
      }
    }
  }
}

