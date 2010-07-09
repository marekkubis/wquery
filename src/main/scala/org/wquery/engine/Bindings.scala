package org.wquery.engine

import scala.collection.mutable.Map
import org.wquery.model.DataType

class Bindings(parent: Option[Bindings]) {
  val bindings = Map[String, Any]()  
  
  def bind(name: String, value: Any) = (bindings(name) = value)
  
  def lookup(name: String): Option[Any] = {        
    if (bindings contains name) {
      Some(bindings(name))
    } else {
      parent match {
        case Some(parent) =>
          parent.lookup(name)
        case None =>
          None
      }      
    }
  }
}
