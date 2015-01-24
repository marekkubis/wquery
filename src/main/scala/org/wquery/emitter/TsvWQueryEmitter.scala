// scalastyle:off multiple.string.literals

package org.wquery.emitter

import org.wquery.lang.{Answer, Error, Result}
import org.wquery.model._

class TsvWQueryEmitter(escaping: Boolean = false) extends WQueryEmitter {
  def emit(result: Result):String = {
    result match {
      case Answer(wordNet, dataSet) =>
        emitDataSet(wordNet, dataSet)
      case Error(exception) =>
        "ERROR: " + exception.getMessage + "\n"
    }
  }

  private def emitDataSet(wordNet: WordNet, dataSet: DataSet): String = {
    val content = dataSet.paths

    if (content.nonEmpty) {
      val builder = new StringBuilder

      for (i <- 0 until content.size) {
        val tuple = content(i)

        emitTuple(wordNet, tuple, builder)
        builder append "\n"
      }

      builder.toString()
    } else {
      "\n"
    }
  }

  private def emitTuple(wordNet: WordNet, tuple: List[Any], builder: StringBuilder) {
    if (tuple.nonEmpty) {
      emitElement(wordNet, tuple.head, builder)

      for (i <- 1 until tuple.size) {
        builder append "\t"
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
      case _ =>
        builder append element
    }
  }

  private def emitSense(sense: Sense, builder: StringBuilder) {
    builder append sense.wordForm
    builder append ":"
    builder append sense.senseNumber
    builder append ":"
    builder append sense.pos
  }
}
