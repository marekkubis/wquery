package org.wquery.model
import org.wquery.WQueryStepVariableCannotBeBoundException
import org.wquery.lang.operations.{ProvidesTupleSizes, ProvidesTypes}
import org.wquery.path.VariableTemplate

import scala.collection.mutable.ListBuffer
import scalaz.Scalaz._

class DataSet(val paths: List[List[Any]], val pathVars: Map[String, List[(Int, Int)]], val stepVars: Map[String, List[Int]])
 extends ProvidesTypes with ProvidesTupleSizes {
  lazy val (minTupleSize, maxTupleSize) = {
    val sizes = paths.map(_.size)
    (if (sizes.isEmpty) 0 else sizes.min, some(if (sizes.isEmpty) 0 else sizes.max))
  }

  val pathCount = paths.size
  val containsSingleTuple = pathCount == 1
  lazy val containsValues = minTupleSize == 1 && maxTupleSize.get == 1
  lazy val containsSingleValue = containsSingleTuple && containsValues
  val isEmpty = pathCount == 0

  lazy val isTrue = {
    if (paths.isEmpty) {
      false
    } else {
      if (maxTupleSize.get == 1) {
        val booleans = paths.map { x =>
          if (x.head.isInstanceOf[Boolean])
            x.head.asInstanceOf[Boolean]
          else
            true
        }

        booleans.forall(x => x)
      } else {
        maxTupleSize.get > 1
      }
    }
  }

  def asValueOf[A]: A = {
    if (containsSingleValue)
      paths.head.head.asInstanceOf[A]
    else
      throw new RuntimeException("DataSet does not contain single value")
  }

  def leftType(pos: Int): Set[DataType] = paths.filter(pos < _.size).map(tuple => DataType.fromValue(tuple(pos))).toSet

  def rightType(pos: Int): Set[DataType] = paths.filter(pos < _.size).map(tuple => DataType.fromValue(tuple(tuple.size - 1 - pos))).toSet

  def types = (for (i <- maxTupleSize.get - 1 to 0 by -1) yield rightType(i)).toList

  def slice(from: Int, until: Option[Int]) = {
    val pathBuffer = DataSetBuffers.createPathBuffer

    for (path <- paths) {
      val pathSlice = path.slice(from, until.getOrElse(path.length))

      if (pathSlice.nonEmpty)
        pathBuffer.append(pathSlice)
    }

    DataSet.fromBuffers(pathBuffer, Map.empty, Map.empty)
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

  override def toString = paths.toString()

  def distinct = DataSet.fromBoundPaths(toBoundPaths.distinct)

  /**
   * multiset equality
   */
  def mequal(dataSet: DataSet) = {
    val pathGroups = paths.groupBy(x => x)
    val dataSetPathGroups = dataSet.paths.groupBy(x => x)

    isSubgroupOf(pathGroups, dataSetPathGroups) && isSubgroupOf(dataSetPathGroups, pathGroups)
  }

  private def isSubgroupOf(leftGroups: Map[scala.List[Any], List[scala.List[Any]]], rightGroups: Map[scala.List[Any], List[scala.List[Any]]]) = {
    leftGroups.forall{ case (left, leftValues) => rightGroups.get(left).some(_.size == leftValues.size).none(false) }
  }

  def bindVariables(variables: VariableTemplate) = {
    if (variables != VariableTemplate.empty) {
      val pathVarBuffers = DataSetBuffers.createPathVarBuffers(variables.pathVariableName.toSet)
      val stepVarBuffers = DataSetBuffers.createStepVarBuffers(variables.stepVariableNames)

      variables.pathVariablePosition match {
        case Some(pathVarPos) =>
          val pathVarBuffer = variables.pathVariableName.map(pathVarBuffers(_))
          val pathVarStart = variables.leftVariablesIndexes.size
          val pathVarEnd = variables.rightVariablesIndexes.size

          for (tuple <- paths) {
            paths.foreach(tuple => bindVariablesFromRight(variables, stepVarBuffers, tuple.size))
            pathVarBuffer.map(_.append((pathVarStart, tuple.size - pathVarEnd)))
            paths.foreach(tuple => bindVariablesFromLeft(variables, stepVarBuffers, tuple.size))
          }
        case None =>
          paths.foreach(tuple => bindVariablesFromRight(variables, stepVarBuffers, tuple.size))
      }

      DataSet(paths, pathVars ++ pathVarBuffers.mapValues(_.toList), stepVars ++ stepVarBuffers.mapValues(_.toList))
    } else {
      this
    }
  }

  private def bindVariablesFromLeft(variables: VariableTemplate, varIndexes: Map[String, ListBuffer[Int]], tupleSize: Int) {
    for ((name, pos) <- variables.leftVariablesIndexes)
      if (pos < tupleSize)
        varIndexes(name).append(pos)
      else
        throw new WQueryStepVariableCannotBeBoundException(name)
  }

  private def bindVariablesFromRight(variables: VariableTemplate, varIndexes: Map[String, ListBuffer[Int]], tupleSize: Int) {
    for ((name, pos) <- variables.rightVariablesIndexes) {
      val index = tupleSize - 1 - pos

      if (index >= 0)
        varIndexes(name).append(index)
      else
        throw new WQueryStepVariableCannotBeBoundException(name)
    }
  }
}

object DataSet {
  val empty = new DataSet(Nil, Map(), Map())

  implicit def DataSetZero = zero(empty)

  def apply(paths: List[List[Any]]) = new DataSet(paths, Map(), Map())

  def apply(paths: List[List[Any]], pathVars: Map[String, List[(Int, Int)]], stepVars: Map[String, List[Int]]) = new DataSet(paths, pathVars, stepVars)

  def fromBoundPaths(boundPaths: List[(List[Any], Map[String, (Int, Int)], Map[String, Int])]) = {
    val pathVarNames = if (boundPaths.isEmpty) Set.empty[String] else boundPaths.head._2.keySet
    val stepVarNames = if (boundPaths.isEmpty) Set.empty[String] else boundPaths.head._3.keySet
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

  def fromOptionalValue(option: Option[Any]) = option.some(fromValue(_)).none(empty)
}

object DataSetBuffers {
  def createPathBuffer = new ListBuffer[List[Any]]

  def createPathVarBuffers(pathVarNames: Set[String]) = {
    Map[String, ListBuffer[(Int, Int)]](pathVarNames.toSeq.map(x => (x, new ListBuffer[(Int, Int)])): _*)
  }

  def createStepVarBuffers(stepVarNames: Set[String]) = {
    Map[String, ListBuffer[Int]](stepVarNames.toSeq.map(x => (x, new ListBuffer[Int])): _*)
  }
}

class DataSetBuffer {
  val pathBuffer = DataSetBuffers.createPathBuffer
  var pathVarBuffers = Map[String, ListBuffer[(Int, Int)]]()
  var stepVarBuffers = Map[String, ListBuffer[Int]]()

  def append(result: DataSet) {
    if (pathBuffer.isEmpty) {
      pathVarBuffers = DataSetBuffers.createPathVarBuffers(result.pathVars.keySet)
      stepVarBuffers = DataSetBuffers.createStepVarBuffers(result.stepVars.keySet)
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

