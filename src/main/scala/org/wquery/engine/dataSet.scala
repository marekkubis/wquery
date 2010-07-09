package org.wquery.engine

import org.wquery.model._

import scala.collection.mutable.ListBuffer

class DataSet(t: List[DataType], c: List[List[Any]]) {
  val types = t
  val content = c    
  
  def isTrue = {    
    if (content.isEmpty) {
        false
    } else {
      if (types.size == 1) {
        if (types.first == BooleanType) 
          content.forall(x => x.first.asInstanceOf[Boolean])
        else 
          true         
      } else { 
        types.size > 1
      }
    }       
  }
  
  override def equals(obj: Any) = {
    obj match {
      case result: DataSet => types == result.types && content == result.content
      case _ => false
    }
  }  
}

object DataSet {
  val empty = new DataSet(Nil, Nil) 
  
  def apply(types: List[DataType], content: List[List[Any]]) = new DataSet(types, content)

  def fromList(vtype: DataType, vlist: List[Any]) = new DataSet(List(vtype), vlist.map(x => List(x)))  
  
  def fromTuple(tuple: List[Any]) = new DataSet(tuple.map{x => DataType(x)}, List(tuple))
  
  def fromValue(value: Any) = new DataSet(List(DataType(value)), List(List(value)))
  
  def fromOptionalValue(option: Option[Any], dtype: DataType) = option match {    
    case Some(value) => 
      fromValue(value)
    case None => 
      new DataSet(List(dtype), Nil)    
  }  
}

class DataSetBuffer { 
  val buffer = new ListBuffer[List[Any]]
  var types = List[DataType]()  
  
  def append(result: DataSet) {
    if (types.size >= result.types.size)
      types = unifyTypes(types, result.types)
    else
      types = unifyTypes(result.types, types)
    
    buffer.appendAll(result.content)
  }
  
  def toDataSet = DataSet(types, buffer.toList)  
  
  private def unifyTypes(left: List[DataType], right: List[DataType]) = {
    val tbuffer = new ListBuffer[DataType]
    
    for (i <- 0 until right.size) {
      if(left(i) == right(i)) {
        tbuffer.append(left(i))
      } else {
        left(i) match {
          case UnionType(ltypes) => {
            right(i) match {
              case UnionType(rtypes) => {
                tbuffer.append(UnionType(ltypes ++ rtypes))
              }
              case rtype @ BasicDataType() => {
                tbuffer.append(UnionType(ltypes + rtype))
              }
            }            
          } 
          case ltype @ BasicDataType() => {
            right(i) match {
              case UnionType(rtypes) => {
                tbuffer.append(UnionType(rtypes + ltype))
              }
              case rtype @ BasicDataType() => {
                tbuffer.append(UnionType(Set(ltype, rtype)))                
              }
            }                        
          }
        }       
      }
    }
    
    for (i <- right.size until left.size) {
      tbuffer.append(left(i))
    }
    
    tbuffer.toList    
  }
}
 
