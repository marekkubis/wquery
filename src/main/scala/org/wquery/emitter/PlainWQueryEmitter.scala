// scalastyle:off multiple.string.literals

package org.wquery.emitter

import org.wquery.lang.Answer
import org.wquery.lang.Error
import org.wquery.lang.Result
import org.wquery.model._

class PlainWQueryEmitter extends WQueryEmitter {
  def emit(result: Result):String = {
    result match {
      case Answer(wordNet, dataSet) =>
        emitDataSet(wordNet, dataSet)
      case Error(exception) =>
        "ERROR: " + exception.getMessage
    }
  }

  private def emitDataSet(wordNet: WordNet, dataSet: DataSet): String = {
    val content = dataSet.paths
    val pathVarNames = dataSet.pathVars.keys.toSeq.filterNot(_.startsWith("_")).sortWith((x, y) => x < y)
    val stepVarNames = dataSet.stepVars.keys.toSeq.filterNot(_.startsWith("_")).sortWith((x, y) => x < y)

    if (!content.isEmpty) {
      val builder = new StringBuilder

      for (i <- 0 until content.size) {
          val tuple = content(i)

        for (pathVarName <- pathVarNames) {
          val varPos = dataSet.pathVars(pathVarName)(i)

          builder append "@"
          builder append pathVarName
          builder append "="
          builder append "("
          emitTuple(wordNet, tuple.slice(varPos._1, varPos._2), builder)
          builder append ")"
          builder append " "
        }

        for (stepVarName <- stepVarNames) {
          builder append "$"
          builder append stepVarName
          builder append "="
          emitElement(wordNet, tuple(dataSet.stepVars(stepVarName)(i)), builder)
          builder append " "
        }

        emitTuple(wordNet, tuple, builder)
        builder append "\n"
      }

      builder.toString
    } else {
      "(no result)\n"
    }
  }

  private def emitTuple(wordNet: WordNet, tuple: List[Any], builder: StringBuilder) {
    if (!tuple.isEmpty) {
        emitElement(wordNet, tuple.head, builder)

        for (i <- (1 until tuple.size)) {
          builder append " "
          emitElement(wordNet, tuple(i), builder)
        }
    }
  }

  private def emitElement(wordNet: WordNet, element: Any, builder: StringBuilder) {
    element match {
      case element: Synset =>
        builder append "{ "

        wordNet.getSenses(element).foreach{ sense =>
          emitSense(sense, builder)
          builder append " "
        }

        builder append "}"
      case element: Sense =>
        emitSense(element, builder)
      case element: Arc =>
        if (element.isCanonical || element.isInverse) {
          if (element.isInverse)
            builder append "^"

          builder append element.relation.name
        } else {
          builder append element.from
          builder append "^"
          builder append element.relation.name
          builder append "^"
          builder append element.to
        }
      case element: String =>
        if (element.indexOf(' ') != -1) {
          builder append "'"
          builder append element
          builder append "'"
        } else {
          builder append element
        }
      case _ =>
        builder append element
    }
  }

  private def emitSense(sense: Sense, builder: StringBuilder) {
    builder append sense.wordForm append ":" append sense.senseNumber append ":" append sense.pos
  }

}
