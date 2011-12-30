package org.wquery.emitter

import org.wquery.engine.Answer
import org.wquery.engine.Error
import org.wquery.engine.Result
import org.wquery.model._

class RawWQueryEmitter extends WQueryEmitter {
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
          emitElement(wordNet, tuple(i), builder)
        }
    }      
  }        
    
  private def emitElement(wordNet: WordNet, element: Any, builder: StringBuilder) {
    element match {
      case element: Synset =>
        builder append element.id
      case element: Sense =>
        emitSense(element, builder)
      case element: Arc =>
        builder append element.from
        builder append "^"
        builder append element.relation.name
        builder append "^"
        builder append element.to
      case element: String =>
        builder append element
      case _ =>
        builder append element      
    }
  }
  
  private def emitSense(sense: Sense, builder: StringBuilder) {
    builder append sense.wordForm append ":" append sense.senseNumber append ":" append sense.pos
  }
}
