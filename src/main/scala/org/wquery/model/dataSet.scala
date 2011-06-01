package org.wquery.model
import scala.collection.mutable.ListBuffer
import java.lang.IllegalArgumentException

class DataSet(val paths: List[List[Any]], val pathVars: Map[String, List[(Int, Int)]], val stepVars: Map[String, List[Int]]) {
  val minPathSize = {// TODO optimize these two
    val sizes = paths.map(_.size)
    if (sizes.size > 0) sizes.min else 0
  }

  val maxPathSize = {// TODO optimize these two
    val sizes = paths.map(_.size)
    if (sizes.size > 0) sizes.max else 0
  }
        
  val pathCount = paths.size
  val isEmpty = pathCount == 0

  def isTrue = {
    if (paths.isEmpty) {
      false
    } else {
      if (maxPathSize == 1) {
        val booleans = paths.map { x => 
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
    val dataTypes: List[BasicType] = paths.map { tuple =>
      BasicType(tuple(tuple.size - 1 - pos))        
    }.distinct
    
    if (dataTypes.size == 1) {
      dataTypes(0)
    } else {
      UnionType(dataTypes.toSet)
    }
  }
  
  def toBoundPaths: List[(List[Any], Map[String, (Int, Int)], Map[String, Int])] = {
      val buffer = new ListBuffer[(List[Any], Map[String, (Int, Int)], Map[String, Int])]
      val pathVarNames = pathVars.keys
      val stepVarNames = stepVars.keys
      
      for (i <- 0 until paths.size) {
        val path = paths(i)
        val pathVarsMap = Map(pathVarNames.map(v => (v, pathVars(v)(i))).toSeq: _*)
        val stepVarsMap = Map(stepVarNames.map(v => (v, stepVars(v)(i))).toSeq: _*)
        
        buffer.append((path, pathVarsMap, stepVarsMap))
      }
      
      buffer.toList
  }

  def isNumeric(pos: Int) = {
    val dataType = getType(pos)
    dataType == IntegerType || dataType == FloatType || dataType == UnionType(Set(IntegerType, FloatType))
  }

  override def toString = paths.toString
}

object DataSet {
  val empty = new DataSet(Nil, Map(), Map())

  def apply(paths: List[List[Any]]) = new DataSet(paths, Map(), Map())
  
  def apply(paths: List[List[Any]], pathVars: Map[String, List[(Int, Int)]], stepVars: Map[String, List[Int]]) = new DataSet(paths, pathVars, stepVars)

  def fromBoundPaths(boundPaths: List[(List[Any], Map[String, (Int, Int)], Map[String, Int])]) = {
    val pathVarNames = if (boundPaths.isEmpty) Nil else boundPaths.head._2.keys.toSeq 
    val stepVarNames = if (boundPaths.isEmpty) Nil else boundPaths.head._3.keys.toSeq
    val pathBuffer = DataSetBuffers.createPathBuffer
    val pathVarBuffers = DataSetBuffers.createPathVarBuffers(pathVarNames)
    val stepVarBuffers = DataSetBuffers.createStepVarBuffers(stepVarNames)    
    
    for ((path, pathVars, stepVars) <- boundPaths) {
      pathBuffer.append(path)
      
      for ((v, pos) <- pathVars)
        pathVarBuffers(v).append(pos)

      for ((v, pos) <- stepVars)
        stepVarBuffers(v).append(pos)
    }
    
    fromBuffers(pathBuffer, pathVarBuffers, stepVarBuffers)
  }
  
  def fromBuffers(pathBuffer: ListBuffer[List[Any]], pathVarBuffers: Map[String, ListBuffer[(Int, Int)]], stepVarBuffers: Map[String, ListBuffer[Int]]) = {
      new DataSet(pathBuffer.toList, pathVarBuffers.mapValues(_.toList), stepVarBuffers.mapValues(_.toList)) 
  }
  
  def fromList(vlist: List[Any]) = new DataSet(vlist.map(List(_)), Map(), Map())

  def fromTuple(tuple: List[Any]) = new DataSet(List(tuple), Map(), Map())

  def fromValue(value: Any) = new DataSet(List(List(value)), Map(), Map())

  def fromOptionalValue(option: Option[Any]) = option.map(fromValue(_)).getOrElse(empty)      
}

object DataSetBuffers {
  def createPathBuffer = new ListBuffer[List[Any]]  
    
  def createPathVarBuffers(pathVarNames: Seq[String]) = {
    Map[String, ListBuffer[(Int, Int)]](pathVarNames.map(x => (x, new ListBuffer[(Int, Int)])): _*)
  }
  
  def createStepVarBuffers(stepVarNames: Seq[String]) = {
    Map[String, ListBuffer[Int]](stepVarNames.map(x => (x, new ListBuffer[Int])): _*)
  }
}

class DataSetBuffer {
  val pathBuffer = DataSetBuffers.createPathBuffer
  var pathVarBuffers = Map[String, ListBuffer[(Int, Int)]]()
  var stepVarBuffers = Map[String, ListBuffer[Int]]()
  
  def append(result: DataSet) {
    if (pathBuffer.isEmpty) {
      pathVarBuffers = DataSetBuffers.createPathVarBuffers(result.pathVars.keys.toSeq)
      stepVarBuffers = DataSetBuffers.createStepVarBuffers(result.stepVars.keys.toSeq)
    }

    if (result.pathVars.keySet != pathVarBuffers.keySet)
      throw new IllegalArgumentException("pathVars.keySet != pathVarBuffers.keySet")

    if (result.stepVars.keySet != stepVarBuffers.keySet)
      throw new IllegalArgumentException("stephVars.keySet != stepVarBuffers.keySet")

    pathBuffer.appendAll(result.paths)
    pathVarBuffers.keys.foreach(key => pathVarBuffers(key).appendAll(result.pathVars(key)))
    stepVarBuffers.keys.foreach(key => stepVarBuffers(key).appendAll(result.stepVars(key)))
  }
  
  def toDataSet = DataSet.fromBuffers(pathBuffer, pathVarBuffers, stepVarBuffers)
}

