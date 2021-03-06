package org.wquery.emitter

import org.wquery.lang.{Answer, Error, Result}
import org.wquery.model._

class XmlWQueryEmitter extends WQueryEmitter {
  def emit(result: Result):String = {
    result match {
      case Answer(wordNet, dataSet) =>
        emitDataSet(wordNet, dataSet)
      case Error(exception) =>
        "<ERROR>" + exception.getMessage + "</ERROR>"
    }
  }

  private def emitDataSet(wordNet: WordNet, dataSet: DataSet): String = {
    val builder = new StringBuilder
    val pathVarNames = dataSet.pathVars.keys.toSeq.filterNot(_.startsWith("_")).sortWith((x, y) => x < y)
    val stepVarNames = dataSet.stepVars.keys.toSeq.filterNot(_.startsWith("_")).sortWith((x, y) => x < y)

    builder append "<RESULTS>\n"

    for (i <- 0 until dataSet.pathCount) {
      builder append "<RESULT>\n"

      val tuple = dataSet.paths(i)

      if (pathVarNames.nonEmpty || stepVarNames.nonEmpty) {
        builder append "<BINDINGS>\n"

        for (pathVarName <- pathVarNames) {
          val varPos = dataSet.pathVars(pathVarName)(i)

          builder append "<PATHVAR name=\""
          builder append pathVarName
          builder append "\">\n"
          emitTuple(wordNet, tuple.slice(varPos._1, varPos._2), builder)
          builder append "</PATHVAR>\n"
        }

        for (stepVarName <- stepVarNames) {
          builder append "<STEPVAR name=\""
          builder append stepVarName
          builder append "\">\n"
          emitElement(wordNet, tuple(dataSet.stepVars(stepVarName)(i)), builder)
          builder append "</STEPVAR>\n"
        }
      }

      builder append "</BINDINGS>\n"
      emitTuple(wordNet, tuple, builder)
      builder append "</RESULT>\n"
    }

    builder append "</RESULTS>\n"
    builder.toString()
  }

  private def emitTuple(wordNet: WordNet, tuple: List[Any], builder: StringBuilder) {
    builder append "<TUPLE>\n"
    tuple.foreach(emitElement(wordNet, _, builder))
    builder append "</TUPLE>\n"
  }


  private def emitElement(wordNet: WordNet, element: Any, builder: StringBuilder) {
    element match {
      case element: Synset =>
        builder append "<SYNSET>\n"
        wordNet.getSenses(element).foreach(emitSense(_, builder))
        builder append "</SYNSET>\n"
      case element: Sense =>
        emitSense(element, builder)
      case element: Arc =>
        emitArc(element, builder)
      case _ =>
        builder append "<VALUE>" append element append "</VALUE>\n"
    }
  }

  private def emitSense(sense: Sense, builder: StringBuilder) {
    builder append "<LITERAL sense=\"" append sense.senseNumber append "\">" append sense.wordForm append "</LITERAL>\n"
  }

  private def emitArc(arc: Arc, builder: StringBuilder) {
    builder append "<ARC from=\"" append arc.from append "\" to=\"" append arc.to append "\">" append arc.relation.name append "</ARC>\n"
  }
}
