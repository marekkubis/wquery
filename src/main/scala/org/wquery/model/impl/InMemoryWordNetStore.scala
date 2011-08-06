package org.wquery.model.impl

import collection.mutable.ListBuffer
import org.wquery.model._
import org.wquery.WQueryEvaluationException

class InMemoryWordNetStore extends WordNetStore {
  private val successors = scala.collection.mutable.Map[(Any, Relation, String), List[Map[String, Any]]]()
  private val patterns = scala.collection.mutable.Map[Relation, List[ExtensionPattern]]()
  private var relationsList = List[Relation]()

  def relations = relationsList

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

  def extend(dataSet: DataSet, pattern: ExtensionPattern) = {
    val buffer = new DataSetBuffer

    pattern.extensions.foreach { extension =>
      buffer.append(extendWithTuples(dataSet, extension.relation, pattern.pos, extension.from, extension.to))
      extendWithPatterns(dataSet, extension.relation, pattern.pos, extension.from, extension.to, buffer)
    }

    buffer.toDataSet
  }

  private def extendWithTuples(dataSet: DataSet, relation: Relation, from: Int, through: String, to: List[String]) = {
    if (relation.name != "_") {
      extendWithRelationTuples(dataSet, relation, from, through, to)
    } else {
      val buffer = new DataSetBuffer

      for (relation <- relations if (relation.name != "_") ; source <- relation.argumentNames ;
           destination <- relation.argumentNames if (source != destination))
        buffer.append(extendWithRelationTuples(dataSet, relation, from, source, List(destination)))

      buffer.toDataSet
    }
  }

  private def extendWithRelationTuples(dataSet: DataSet, relation: Relation, from: Int, through: String, to: List[String]) = {
    relation.demandArgument(through)
    to.foreach(relation.demandArgument)

    val pathBuffer = DataSetBuffers.createPathBuffer
    val pathVarBuffers = DataSetBuffers.createPathVarBuffers(dataSet.pathVars.keys.toSeq)
    val stepVarBuffers = DataSetBuffers.createStepVarBuffers(dataSet.stepVars.keys.toSeq)
    val pathVarNames = dataSet.pathVars.keys
    val stepVarNames = dataSet.stepVars.keys

    for (i <- 0 until dataSet.pathCount) {
      val tuple = dataSet.paths(i)
      val source = tuple(tuple.size - 1 - from)

      if (successors.contains((source, relation, through))) {
        for (succs <- successors(source, relation, through)) {
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

  private def extendWithPatterns(dataSet: DataSet, relation: Relation, from: Int, through: String, to: List[String], buffer: DataSetBuffer) {
    if (patterns.contains(relation)) {
      if (through == Relation.Source && to.size == 1 && to.head == Relation.Destination) {
        patterns(relation).foreach{ case ExtensionPattern(_, extensions) =>
          buffer.append(extend(dataSet, ExtensionPattern(from, extensions)))
        }
      } else {
        throw new WQueryEvaluationException("One cannot traverse the inferred relation "
          + relation + " using custom source or destination arguments")
      }
    }
  }

  def add(relation: Relation) {
    relationsList = (relationsList :+ relation).sortWith((l, r) => l.name < r.name || l.name == r.name && l.sourceType < r.sourceType)
  }

  def add(relation: Relation, pattern: ExtensionPattern) {
    if (!(patterns.contains(relation))) {
      patterns(relation) = Nil
    }

    patterns(relation) = patterns(relation) :+ pattern
  }

  def add(relation: Relation, tuple: List[(String, Any)]) {
    for ((sourceName: String, sourceValue: Any) <- tuple) {
      if (!(successors.contains(sourceValue, relation, sourceName))) {
        successors((sourceValue, relation, sourceName)) = Nil
      }

      successors((sourceValue, relation, sourceName)) = tuple.toMap::successors((sourceValue, relation, sourceName))
    }
  }

  def remove(relation: Relation) {
    relationsList = relationsList.filterNot(_ == relation)
    patterns.remove(relation)
    successors.keys.filter { case (_, rel, _) => rel == relation }.foreach(successors.remove(_))
  }
}