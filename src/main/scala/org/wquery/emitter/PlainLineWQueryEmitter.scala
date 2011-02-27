package org.wquery.emitter
import org.wquery.engine.{Result, Error, Answer}
import org.wquery.model.DataSet

class PlainLineWQueryEmitter extends PlainWQueryEmitter {
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
          builder append "@"
          builder append pathVarName
          builder append "="
          emitTuple(dataSet.pathVars(pathVarName), builder)          
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
}
