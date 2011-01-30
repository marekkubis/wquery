package org.wquery.emitter
import org.wquery.engine.{Result, Error, Answer, DataSet}

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
    val content = dataSet.content
    
    if (!content.isEmpty) {
      val builder = new StringBuilder 

      for (tuple <- content) {
        if (!tuple.isEmpty) {
            emitElement(tuple.head, builder)
                    
            for (i <- (1 until tuple.size)) {
              builder append " "
              emitElement(tuple(i), builder) 
            }
        }
        
        builder append "\n"
      }
      
      return builder toString
    } else {
      return "(no result)\n"
    }    
  }
}
