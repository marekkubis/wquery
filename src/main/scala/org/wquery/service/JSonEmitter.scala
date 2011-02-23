package org.wquery.service
import org.wquery.emitter.WQueryEmitter
import org.wquery.engine.{Error, Answer, Result, DataSet}
import org.wquery.model.{Sense, Synset}

class JSonEmitter extends WQueryEmitter {
  def emit(result: Result):String = {
    result match {
      case Answer(dataSet) =>
        emitDataSet(dataSet)
      case Error(exception) =>
        "{error:\""+ exception +"\"}"        
    }
  }  
  
  private def emitDataSet(dataSet: DataSet): String = {
    val content = dataSet.paths 
    val builder = new StringBuilder    
    
    builder append "["     
    
    if (!content.isEmpty) {
        emitTuple(content.head, builder)
        
        for (i <- 1 until content.size) {
          builder append ","
          emitTuple(content(i), builder)
        }
    }

    builder append "]"    
    builder.toString
  }
  
  private def emitTuple(tuple: List[Any], builder: StringBuilder) {
    builder append "["
    
    if (!tuple.isEmpty) {
        emitElement(tuple.head, builder)
        
        for (i <- 1 until tuple.size) {
          builder append ","
          emitElement(tuple(i), builder)
        }
    }    
    
    builder append "]"
  }
  
  private def emitElement(element: Any, builder: StringBuilder) {
    element match {
      case element: Synset =>
        builder append "\""        
        builder append element.id
        builder append "\""        
      case element: Sense =>
        builder append "\""                
        builder append element.id
        builder append "\""                
      case element: String =>
        builder append "\""                
        builder append element
        builder append "\""                
      case element: Int =>
        builder append element
      case element: Double =>
        builder append element
      case element: Boolean =>
        builder append element
    }
  }
}
