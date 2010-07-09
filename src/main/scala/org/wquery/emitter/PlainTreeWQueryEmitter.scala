package org.wquery.emitter

import org.wquery.engine.DataSet
import org.wquery.engine.Answer
import org.wquery.engine.Error
import org.wquery.engine.Result
import org.wquery.model.Synset
import org.wquery.model.Sense

class PlainTreeWQueryEmitter extends PlainWQueryEmitter {
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
      var prevTuple: List[Any] = (1 to dataSet.types.size).toList.map(x => null) 
      var element: Any = null
      var i = 0

      for (tuple <- content) {
        i = 0   
        
        while (i < tuple.size && (tuple(i) == null || (i < prevTuple.size && tuple(i) == prevTuple(i)))) i += 1
        
        if (i < tuple.size) {
          while (i < tuple.size) {
            element = tuple(i)

            if (element != null) {
              (1 to i).foreach(x => builder append "\t")
              emitElement(element, builder) 
              builder append "\n"              
            }

            i += 1
          }
        } else {
          (1 to tuple.size - 2).foreach(x => builder append "\t")
          
          if (! tuple.isEmpty) 
            emitElement(tuple.last, builder)
          
          builder append "\n"
        }

        prevTuple = tuple;
      }
      
      return builder toString
    } else {
        return "(no result)\n"
    }    
  }
}
