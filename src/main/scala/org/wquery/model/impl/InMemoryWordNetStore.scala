package org.wquery.model.impl

import collection.mutable.ListBuffer
import org.wquery.model._

class InMemoryWordNetStore extends WordNetStore {
  private val successors = scala.collection.mutable.Map[(Any, Relation, String), List[Map[String, Any]]]()
  private val relationsSet = scala.collection.mutable.Set[Relation]()

  def relations = relationsSet.toList

  def generate(relation: Relation, from: List[(String, List[Any])], to: List[String]) = {
    val buffer = DataSetBuffers.createPathBuffer
    val (sourceName, sourceValues) = from.head
    val dests = from.tail

    for (((obj, rel, src), destMaps) <- successors) {
      if (rel == relation && src == sourceName && (sourceValues.isEmpty || sourceValues.contains(obj))) {
        for (destMap <- destMaps) {
          if (dests.forall(dest => destMap.contains(dest._1) && (dest._2.isEmpty || dest._2.contains(destMap(dest._1))))) {
            val tupleBuffer = new ListBuffer[Any]

            tupleBuffer.append(destMap(to.head))

            for (destName <- to.tail) {
              tupleBuffer.append(Arc(relation, to.head, destName))
              tupleBuffer.append(destMap(destName))
            }

            buffer.append(tupleBuffer.toList)
          }
        }
      }
    }

    DataSet(buffer.toList)
  }

  def extend(dataSet: DataSet, relation: Relation, from: List[(Int, String)], to: List[String]) = {
    from.map(_._2).foreach(relation.demandArgument)
    to.foreach(relation.demandArgument)

    val pathBuffer = DataSetBuffers.createPathBuffer
    val pathVarBuffers = DataSetBuffers.createPathVarBuffers(dataSet.pathVars.keys.toSeq)
    val stepVarBuffers = DataSetBuffers.createStepVarBuffers(dataSet.stepVars.keys.toSeq)
    val pathVarNames = dataSet.pathVars.keys
    val stepVarNames = dataSet.stepVars.keys

    // TODO for now the method demands exactly one source
    val (pos, source) = from.head

    for (i <- 0 until dataSet.pathCount) {
      val tuple = dataSet.paths(i)

      if (successors.contains((tuple(tuple.size - pos), relation, source))) {
        for (succs <- successors(tuple(tuple.size - pos), relation, source)) {
          val tupleBuffer = new ListBuffer[Any]
          tupleBuffer.appendAll(tuple)
          to.foreach { dest =>
            if (succs.contains(dest)) {
              tupleBuffer.append(Arc(relation, source, dest))
              tupleBuffer.append(succs(dest))
            }
          }

          pathBuffer.append(tupleBuffer.toList)

          for (pathVar <- pathVarNames)
              pathVarBuffers(pathVar).append(dataSet.pathVars(pathVar)(i))

          for (stepVar <- stepVarNames)
              stepVarBuffers(stepVar).append(dataSet.stepVars(stepVar)(i))
        }
      }
    }

    DataSet.fromBuffers(pathBuffer, pathVarBuffers, stepVarBuffers)
  }

  def add(relation: Relation) = relationsSet += relation

  def remove(relation: Relation) = null // TODO remove all relation tuples

  def add(relation: Relation, tuple: List[(String, Any)]) {
    for ((sourceName: String, sourceValue: Any) <- tuple) {
      if (!(successors.contains(sourceValue, relation, sourceName))) {
        successors((sourceValue, relation, sourceName)) = Nil
      }

      successors((sourceValue, relation, sourceName)) = tuple.toMap::successors((sourceValue, relation, sourceName))
    }
  }

  def remove(relation: Relation, tuple: List[(String, Any)]) = null

}