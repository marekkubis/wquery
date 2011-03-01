package org.wquery.emitter

import org.wquery.engine.Answer
import org.wquery.engine.Error
import org.wquery.engine.Result
import org.wquery.model.{Sense, Synset, Arc, NodeType, SynsetType, SenseType, StringType, IntegerType, FloatType, BooleanType, DataSet}

class PlainWQueryEmitter extends WQueryEmitter {
  def emit(result: Result):String = {
    result match {
      case Answer(dataSet) =>
        emitDataSet(dataSet)
      case Error(exception) =>
        "ERROR: " + exception.getMessage
    }
  }  
  
  private def emitDataSet(dataSet: DataSet): String = {
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
          emitTuple(tuple.slice(varPos._1, varPos._2), builder)
          builder append ")"
          builder append " "
        }
        
        for (stepVarName <- stepVarNames) {
          builder append "$"
          builder append stepVarName
          builder append "="
          emitElement(tuple(dataSet.stepVars(stepVarName)(i)), builder)
          builder append " "
        }
          
        emitTuple(tuple, builder)
        builder append "\n"
      }
      
      return builder toString
    } else {
      return "(no result)\n"
    }    
  }
  
  private def emitTuple(tuple: List[Any], builder: StringBuilder) {
    if (!tuple.isEmpty) {
        emitElement(tuple.head, builder)
                
        for (i <- (1 until tuple.size)) {
          builder append " "
          emitElement(tuple(i), builder) 
        }
    }      
  }        
    
  private def emitElement(element: Any, builder: StringBuilder) {
    element match {
      case element: Synset =>
        builder append "{ "
        
        element.senses.foreach{ sense => 
          emitSense(sense, builder)
          builder append " "
        }
        
        builder append "}"             
      case element: Sense => 
        emitSense(element, builder)
      case element: Arc => 
        if (element.isInverse)
          builder append "^"
            
        builder append element.relation.name
        
        if (!element.isCanonical && !element.isInverse) {
          builder append "("
          builder append element.from
          builder append ","        
          builder append element.to
          builder append ")"      
        }
      case element => 
        builder append element      
    }
  }
  
  private def emitSense(sense: Sense, builder: StringBuilder) {
    builder append sense.wordForm append ":" append sense.senseNumber append ":" append sense.pos
  }
  
}
