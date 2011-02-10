package org.wquery.emitter
import org.wquery.model.{Sense, Synset, Arc, NodeType, SynsetType, SenseType, StringType, IntegerType, FloatType, BooleanType}

abstract class PlainWQueryEmitter extends WQueryEmitter {
  protected def emitElement(element: Any, builder: StringBuilder) {
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
  
  protected def emitSense(sense: Sense, builder: StringBuilder) {
    builder append sense.wordForm append ":" append sense.senseNumber append ":" append sense.pos
  }
  
}
