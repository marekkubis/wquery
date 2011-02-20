package org.wquery.engine
import org.wquery.model._
import scala.collection.mutable.ListBuffer

class DataSet(val content: List[List[Any]], stepVars: Map[String, List[Int]], pathVars: Map[String, List[(Int, Int)]]) {
  val minPathSize = {// TODO optimize these two
      val sizes = content.map(x => x.size)
      if (sizes.size > 0) sizes.min else 0
  }

  val maxPathSize = {// TODO optimize these two
      val sizes = content.map(x => x.size)
      if (sizes.size > 0) sizes.max else 0
  }
        
  val pathCount = content.size
  val isEmpty = pathCount == 0

  def isTrue = {
    if (content.isEmpty) {
      false
    } else {
      if (maxPathSize == 1) {
        val booleans = content.map { x => 
          if (x.head.isInstanceOf[Boolean]) 
            x.head.asInstanceOf[Boolean] 
          else 
            true
        }

        booleans.forall(x => x)
      } else {
        maxPathSize > 1
      }
    }
  }
  
  def getType(pos: Int): DataType = { // TODO optimize
    val dataTypes: List[BasicType] = content.map { tuple =>
      DataType(tuple(tuple.size - 1 - pos))        
    }.distinct
    
    if (dataTypes.size == 1) {
      dataTypes(0)
    } else {
      UnionType(dataTypes.toSet)
    }
  }

  def isNumeric(pos: Int) = {
    val dataType = getType(pos)
    dataType == IntegerType || dataType == FloatType || dataType == UnionType(Set(IntegerType, FloatType))
  }
  
  override def equals(obj: Any) = {
    obj match {
      case result: DataSet => content == result.content
      case _ => false
    }
  }
  
  override def toString = content.toString
}

object DataSet {
  val empty = new DataSet(Nil, Map(), Map())

  def apply(content: List[List[Any]]) = new DataSet(content, Map(), Map())
  
  def apply(content: List[List[Any]], stepVars: Map[String, List[Int]], pathVars: Map[String, List[(Int, Int)]]) = new DataSet(content, stepVars, pathVars)

  def fromList(vlist: List[Any]) = new DataSet(vlist.map(x => List(x)), Map(), Map())

  def fromTuple(tuple: List[Any]) = new DataSet(List(tuple), Map(), Map())

  def fromValue(value: Any) = new DataSet(List(List(value)), Map(), Map())

  def fromOptionalValue(option: Option[Any]) = option match {
    case Some(value) =>
      fromValue(value)
    case None =>
      empty
  }
}

class DataSetBuffer { 
  val buffer = new ListBuffer[List[Any]]
  var types = List[DataType]()  
  
  def append(result: DataSet) {    
    buffer.appendAll(result.content)
  }
  
  def toDataSet = DataSet(buffer.toList)    
}

