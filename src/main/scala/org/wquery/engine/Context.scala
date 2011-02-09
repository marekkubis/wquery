package org.wquery.engine
import org.wquery.model.DataType

class Context(val values: List[Any]) {    
  def size = values.size
  
  def isEmpty = values.isEmpty
}

object Context {
  val empty = new Context(Nil)  
  
  def apply(values: List[Any]) = new Context(values)
}