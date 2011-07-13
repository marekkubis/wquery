package org.wquery.model.impl

import collection.mutable.ListBuffer
import org.wquery.model._

class InMemoryWordNetStore extends WordNetStore {
  private val successors = scala.collection.mutable.Map[(Any, Relation, String), List[Map[String, Any]]]()
  private val relationsSet = scala.collection.mutable.Set[Relation]()

  def relations = relationsSet.toList

  def fetch(relation: Relation, from: List[(String, List[Any])], to: List[String]) = {
    val buffer = DataSetBuffers.createPathBuffer
    val (sourceName, sourceValues) = from.head
    val destinations = from.tail

    if (sourceValues.isEmpty) {
      for (((obj, rel, src), destinationMaps) <- successors if (rel == relation && src == sourceName))
        appendDestinationTuples(destinationMaps, destinations, to, relation, buffer)
    } else {
      for (sourceValue <- sourceValues)
        successors.get((sourceValue, relation, sourceName))
          .map(appendDestinationTuples(_, destinations, to, relation, buffer))
    }

    DataSet(buffer.toList)
  }

  private def appendDestinationTuples(destinationMaps: List[Map[String, Any]], destinations: List[(String, List[Any])], to: List[String], relation: Relation, buffer: ListBuffer[List[Any]]) {
    for (destinationMap <- destinationMaps) {
      if (destinations.forall(dest => destinationMap.contains(dest._1) && (dest._2.isEmpty || dest._2.contains(destinationMap(dest._1))))) {
        val tupleBuffer = new ListBuffer[Any]

        tupleBuffer.append(destinationMap(to.head))

        for (destinationName <- to.tail) {
          tupleBuffer.append(Arc(relation, to.head, destinationName))
          tupleBuffer.append(destinationMap(destinationName))
        }

        buffer.append(tupleBuffer.toList)
      }
    }
  }

  def extend(dataSet: DataSet, relation: Relation, from: Int, through: String, to: List[String]) = {
    relation.demandArgument(through)
    to.foreach(relation.demandArgument)

    val pathBuffer = DataSetBuffers.createPathBuffer
    val pathVarBuffers = DataSetBuffers.createPathVarBuffers(dataSet.pathVars.keys.toSeq)
    val stepVarBuffers = DataSetBuffers.createStepVarBuffers(dataSet.stepVars.keys.toSeq)
    val pathVarNames = dataSet.pathVars.keys
    val stepVarNames = dataSet.stepVars.keys

    for (i <- 0 until dataSet.pathCount) {
      val tuple = dataSet.paths(i)

      if (successors.contains((tuple(tuple.size - from), relation, through))) {
        for (succs <- successors(tuple(tuple.size - from), relation, through)) {
          val tupleBuffer = new ListBuffer[Any]
          tupleBuffer.appendAll(tuple)
          to.foreach { dest =>
            if (succs.contains(dest)) {
              tupleBuffer.append(Arc(relation, through, dest))
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