package org.wquery.engine
import org.wquery.model.DataType

class Context(t: List[DataType], v: List[Any]) {
  val types = t
  val values = v  
  
  def size = t.size
  
  def isEmpty = t.isEmpty
}

object Context {
  val empty = new Context(Nil, Nil)  
  
  def apply(types: List[DataType], values: List[Any]) = new Context(types, values)
}