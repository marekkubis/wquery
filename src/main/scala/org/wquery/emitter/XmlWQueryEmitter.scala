package org.wquery.emitter
import org.wquery.engine.{Result, Error, Answer, DataSet}
import org.wquery.model.{Sense, Synset}

class XmlWQueryEmitter extends WQueryEmitter {
  def emit(result: Result):String = {
    result match {
      case Answer(dataSet) =>
        emitDataSet(dataSet)
      case Error(exception) =>
        "<ERROR>" + exception.getMessage + "</ERROR>"
    }
  }  
  
  private def emitDataSet(dataSet: DataSet): String = {
    val builder = new StringBuilder
    builder append "<RESULT>\n" 
      
    dataSet.content.foreach { tuple => 
      builder append "<TUPLE>\n"
      tuple.foreach {elem => emitElement(elem, builder)}
      builder append "</TUPLE>\n"        
    }
  
    builder append "</RESULT>"    
    builder.toString
  }
  
  private def emitElement(element: Any, builder: StringBuilder) {
    element match {
      case element: Synset =>
        builder append "<SYNSET>\n"
        
        element.senses.foreach{ sense => 
          emitSense(sense, builder)
          builder append " "
        }
        
        builder append "</SYNSET>\n"        
      case element: Sense => 
        emitSense(element, builder)      
      case element => 
        builder append "<VALUE>" append element append "</VALUE>\n"      
    }
  }
  
  private def emitSense(sense: Sense, builder: StringBuilder) {
    builder append "<LITERAL sense=\"" append sense.senseNumber append "\">" append sense.wordForm append "</ILR>\n" 
  }
}
