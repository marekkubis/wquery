package org.wquery.emitter

import org.wquery.model.Arc
import org.wquery.engine.{Result, Error, Answer}
import org.wquery.model.{Sense, Synset, DataSet}

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
    val pathVarNames = dataSet.pathVars.keys.toSeq.filterNot(_.startsWith("_")).sortWith((x, y) => x < y)
    val stepVarNames = dataSet.stepVars.keys.toSeq.filterNot(_.startsWith("_")).sortWith((x, y) => x < y)
    
    builder append "<RESULTS>\n"
            
    for (i <- 0 until dataSet.pathCount) {
      builder append "<RESULT>\n"        
        
      val tuple = dataSet.paths(i)      
    
      if (!pathVarNames.isEmpty || !stepVarNames.isEmpty) {
        builder append "<BINDINGS>\n"  
          
        for (pathVarName <- pathVarNames) {
          val varPos = dataSet.pathVars(pathVarName)(i)   
            
          builder append "<PATHVAR name=\""
          builder append pathVarName
          builder append "\">\n"
          emitTuple(tuple.slice(varPos._1, varPos._2), builder)
          builder append "</PATHVAR>\n"
        }
        
        for (stepVarName <- stepVarNames) {
          builder append "<STEPVAR name=\""
          builder append stepVarName
          builder append "\">\n"
          emitElement(tuple(dataSet.stepVars(stepVarName)(i)), builder)
          builder append "</STEPVAR>\n"
        }
      }
      
      builder append "</BINDINGS>\n"                
      emitTuple(tuple, builder)
      builder append "</RESULT>\n"
    }    

    builder append "</RESULTS>\n"    
    builder.toString
  }
    
  private def emitTuple(tuple: List[Any], builder: StringBuilder) {
    builder append "<TUPLE>\n"    
    tuple.foreach(emitElement(_, builder))    
    builder append "</TUPLE>\n"    
  }        
        
  
  private def emitElement(element: Any, builder: StringBuilder) {
    element match {
      case element: Synset =>
        builder append "<SYNSET>\n"        
        element.senses.foreach(emitSense(_, builder))        
        builder append "</SYNSET>\n"        
      case element: Sense => 
        emitSense(element, builder)      
      case element: Arc =>
        emitArc(element, builder)
      case element => 
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
