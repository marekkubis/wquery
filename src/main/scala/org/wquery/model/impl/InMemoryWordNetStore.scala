package org.wquery.model.impl

import collection.mutable.ListBuffer
import org.wquery.model._
import org.wquery.{WQueryUpdateBreaksRelationPropertyException, WQueryModelException, WQueryEvaluationException}
import akka.stm._
import org.wquery.engine.{NewSynset, Bindings, RelationalPattern}

class InMemoryWordNetStore extends WordNetStore {
  private val successors = TransactionalMap[Relation, TransactionalMap[(String, Any), IndexedSeq[Map[String, Any]]]]()
  private val patterns = scala.collection.mutable.Map[Relation, List[RelationalPattern]]()
  private var relationsList = List[Relation]()

  def relations = relationsList

  def addRelation(relation: Relation) {
    if (!relations.contains(relation)) {
      relationsList = (relationsList :+ relation).sortWith((l, r) => l.name < r.name || l.name == r.name && l.arguments.size < r.arguments.size || l.name == r.name && l.arguments.size == r.arguments.size && l.sourceType < r.sourceType)
      successors(relation) = TransactionalMap()
      dependent(relation) = Set.empty
      collectionDependent(relation) = Set.empty
      functionalFor(relation) = Set.empty
      requiredBys(relation) = Set.empty
      transitives(relation) = false
      symmetry(relation) = NonSymmetric
    }
  }

  def removeRelation(relation: Relation) {
    if (WordNet.relations.contains(relation))
      throw new WQueryModelException("Cannot remove the mandatory relation '" + relation + "'")

    relationsList = relationsList.filterNot(_ == relation)
    patterns.remove(relation)
    successors.remove(relation)
  }

  def setRelations(newRelations: List[Relation], assignments: List[PropertyAssignment]) {
    if (WordNet.relations.exists(relation => !newRelations.contains(relation)))
      throw new WQueryModelException("Assignment removes a mandatory relation")

    for (relation <- relations if (!newRelations.contains(relation)))
      removeRelation(relation)

    for (relation <- newRelations if (!relations.contains(relation)))
      addRelation(relation)
  }

  private def follow(relation: Relation, from: String, through: Any, to: String) = {
    successors(relation).get((from, through)).map(_.map(succ => succ(to))).getOrElse(IndexedSeq.empty)
  }

  def getSenses(synset: Synset) = follow(WordNet.SynsetToSenses, Relation.Source, synset, Relation.Destination).toList.asInstanceOf[List[Sense]]

  def getSynset(sense: Sense) = {
    val synsets = follow(WordNet.SenseToSynset, Relation.Source, sense, Relation.Destination).toList

    if (synsets.isEmpty) None else Some(synsets.head.asInstanceOf[Synset])
  }

  def fetch(relation: Relation, from: List[(String, List[Any])], to: List[String]) = {
    val buffer = DataSetBuffers.createPathBuffer
    val (sourceName, sourceValues) = from.head
    val destinations = from.tail

    if (sourceValues.isEmpty) {
      for (((src, obj), destinationMaps) <- successors(relation) if (src == sourceName))
        appendDestinationTuples(destinationMaps, destinations, to, relation, buffer)
    } else {
      for (sourceValue <- sourceValues)
        successors(relation).get((sourceName, sourceValue))
          .map(appendDestinationTuples(_, destinations, to, relation, buffer))
    }

    DataSet(buffer.toList)
  }

  private def appendDestinationTuples(destinationMaps: Seq[Map[String, Any]], destinations: List[(String, List[Any])], to: List[String], relation: Relation, buffer: ListBuffer[List[Any]]) {
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

  def extend(dataSet: DataSet, from: Int, through: (String, Option[NodeType]), to: List[(String, Option[NodeType])]): DataSet = {
    val buffer = new DataSetBuffer
    val toMap = to.toMap

    for (relation <- relations if relation.arguments.size > 1;
         source <- relation.argumentNames if through._1 == "_" || through._1 == source;
         destination <- relation.argumentNames if toMap.isEmpty || toMap.get(destination).map(nodeTypeOption => nodeTypeOption.map(_ == relation.demandArgument(destination).nodeType).getOrElse(true)).getOrElse(false) && source != destination)
      buffer.append(extendWithRelationTuples(dataSet, relation, from, source, List(destination)))

    buffer.toDataSet
  }

  def extend(dataSet: DataSet, relation: Relation, from: Int, through: String, to: List[String]) = {
    val buffer = new DataSetBuffer

    buffer.append(extendWithRelationTuples(dataSet, relation, from, through, to))
    extendWithPatterns(dataSet, relation, from, through, to, buffer)
    buffer.toDataSet
  }

  private def extendWithRelationTuples(dataSet: DataSet, relation: Relation, from: Int, through: String, to: List[String]) = {
    relation.demandArgument(through)
    to.foreach(relation.demandArgument)

    val relationSuccessors = successors(relation)
    val pathBuffer = DataSetBuffers.createPathBuffer
    val pathVarBuffers = DataSetBuffers.createPathVarBuffers(dataSet.pathVars.keys.toSeq)
    val stepVarBuffers = DataSetBuffers.createStepVarBuffers(dataSet.stepVars.keys.toSeq)
    val pathVarNames = dataSet.pathVars.keys
    val stepVarNames = dataSet.stepVars.keys

    for (i <- 0 until dataSet.pathCount) {
      val tuple = dataSet.paths(i)
      val source = tuple(tuple.size - 1 - from)

      for (relSuccs <- relationSuccessors.get((through, source)); succs <- relSuccs) {
        val tupleBuffer = new ListBuffer[Any]

        for(destination <- to if (succs.contains(destination))) {
          tupleBuffer.append(Arc(relation, through, destination))
          tupleBuffer.append(succs(destination))
        }

        if (tupleBuffer.size == 2 * to.size) {
          tupleBuffer.prependAll(tuple)
          pathBuffer.append(tupleBuffer.toList)
        }

        for (pathVar <- pathVarNames)
            pathVarBuffers(pathVar).append(dataSet.pathVars(pathVar)(i))

        for (stepVar <- stepVarNames)
            stepVarBuffers(stepVar).append(dataSet.stepVars(stepVar)(i))
      }
    }

    DataSet.fromBuffers(pathBuffer, pathVarBuffers, stepVarBuffers)
  }

  private def extendWithPatterns(dataSet: DataSet, relation: Relation, from: Int, through: String, to: List[String], buffer: DataSetBuffer) {
    if (patterns.contains(relation)) {
      if (through == Relation.Source && to.size == 1 && to.head == Relation.Destination) {
        patterns(relation).foreach( pattern =>
          buffer.append(pattern.extend(this, Bindings(), dataSet, from))
        )
      } else {
        throw new WQueryEvaluationException("One cannot traverse the inferred relation "
          + relation + " using custom source or destination arguments")
      }
    }
  }

  def addRelationPattern(relation: Relation, pattern: RelationalPattern) {
    if (!(patterns.contains(relation))) {
      patterns(relation) = Nil
    }

    patterns(relation) = patterns(relation) :+ pattern
  }

  def addSense(sense: Sense, assignments: List[PropertyAssignment]) = {
    addSynset(None, List(sense), assignments)
  }

  def addSynset(synsetId: Option[String], senses: List[Sense], assignments: List[PropertyAssignment]) = {
    val synset = new Synset(synsetId.getOrElse("synset#" + senses.head))

    addNode(SynsetType, synset, assignments)
    addSuccessor(synset.id, WordNet.IdToSynset, synset)
    addSuccessor(synset, WordNet.SynsetToId, synset.id)

    for (sense <- senses) {
      getSynset(sense)
        .map(synsetSense => moveSense(sense, synsetSense, synset))
        .getOrElse(createSense(sense, synset, assignments))
    }

    synset
  }

  private def createSense(sense: Sense, synset: Synset, patterns: List[PropertyAssignment]) {
    if (!isWord(sense.wordForm))
      addNode(StringType, sense.wordForm, patterns)

    if (!isPartOfSpeechSymbol(sense.pos))
      addNode(POSType, sense.pos, patterns)

    addNode(SenseType, sense, patterns)
    addSuccessor(sense, WordNet.SenseToWordForm, sense.wordForm)
    addSuccessor(sense, WordNet.SenseToSenseNumber, sense.senseNumber)
    addSuccessor(sense, WordNet.SenseToPos, sense.pos)
    addSuccessor(synset, WordNet.SynsetToSenses, sense)
    addSuccessor(synset, WordNet.SynsetToWordForms, sense.wordForm)
    addSuccessor(sense.wordForm, WordNet.WordFormToSenses, sense)
    addSuccessor(sense, WordNet.SenseToSynset, synset)
    addSuccessor(sense.wordForm, WordNet.WordFormToSynsets, synset)
    addLink(WordNet.SenseToWordFormSenseNumberAndPos, Map((Relation.Source, sense), (Relation.Destination, sense.wordForm), ("num", sense.senseNumber), ("pos", sense.pos)))
  }

  def addWord(word: String, patterns: List[PropertyAssignment]) = addNode(StringType, word, patterns)

  def addPartOfSpeechSymbol(pos: String, patterns: List[PropertyAssignment]) = if (!isPartOfSpeechSymbol(pos)) addNode(POSType, pos, patterns)

  private def isWord(word: String) = {
    successors(WordNet.WordSet).get(Relation.Source, word).map(!_.isEmpty).getOrElse(false)
  }

  private def isPartOfSpeechSymbol(pos: String) = {
    successors(WordNet.PosSet).get(Relation.Source, pos).map(!_.isEmpty).getOrElse(false)
  }

  private def addNode(nodeType: NodeType, node: Any, assignments: List[PropertyAssignment]) = atomic {
    val relation = getRelationByNodeType(nodeType)

    for ((relation, argument) <- getRequiredBys(nodeType)) {
      if (!assignments.exists(pattern => pattern.pattern.relation.get == relation && pattern.pattern.source.name == argument && pattern.op != "-="))
        throw new WQueryModelException("A new " + nodeType + " does not fulfil required by constrains of the relation '" + relation + "' on the argument '" + argument + "'")
    }

    addLink(relation, Map((Relation.Source, node)))

    for (assignment <- assignments) {
      val tuples = assignment.tuplesFor(node)

      assignment.op match {
        case "+=" =>
          tuples.foreach(addLink(assignment.pattern.relation.get, _))
        case ":=" =>
          setLinks(assignment.pattern.relation.get, assignment.pattern.source.name, node, tuples)
        case "-=" =>
          tuples.foreach(removeLink(assignment.pattern.relation.get, _, true))
      }
    }
  }

  private def getRequiredBys(requiredType: NodeType) = {
    relations.flatMap(relation => requiredBys(relation).filter(name => relation.demandArgument(name).nodeType == requiredType).map(name => (relation, name)))
  }

  private def getRelationByNodeType(nodeType: NodeType) = (nodeType: @unchecked) match {
    case SynsetType =>
      WordNet.SynsetSet
    case SenseType =>
      WordNet.SenseSet
    case StringType =>
      WordNet.WordSet
    case POSType =>
      WordNet.PosSet
  }

  def addLink(relation: Relation, tuple: Map[String, Any]) = atomic {
    handleFunctionalForPropertyForAddLink(relation, tuple)
    addEdge(relation, tuple)
    handleSymmetryForAddLink(relation, tuple)
  }

  private def addEdge(relation: Relation, tuple: Map[String, Any]) = atomic {
    val relationSuccessors = successors(relation)

    for ((sourceName, sourceValue) <- tuple) {
      if (!(relationSuccessors.contains((sourceName, sourceValue)))) {
        relationSuccessors((sourceName, sourceValue)) = TransactionalVector()
      }

      relationSuccessors((sourceName, sourceValue)) = relationSuccessors((sourceName, sourceValue)) :+ tuple
    }
  }

  private def containsLink(relation: Relation, tuple: Map[String, Any]) = {
    successors(relation).get(Relation.Source, tuple(Relation.Source)).map(_.contains(tuple)).getOrElse(false)
  }

  private def handleFunctionalForPropertyForAddLink(relation: Relation, tuple: Map[String, Any]) {
    val relationSuccessors = successors(relation)

    for (param <- functionalFor(relation) if (tuple.contains(param))) {
      val node = tuple(param)

      relationSuccessors.get((param, node)).map( successors =>
        if (!successors.isEmpty) {
          if (functionalForActions((relation, param)) == Relation.Preserve)
            throw new WQueryUpdateBreaksRelationPropertyException(Relation.Functional, relation, param)
          else if (functionalForActions((relation, param)) == Relation.Restore)
            successors.foreach(removeLinkByNode(relation, _, Some(node)))
        }
      )
    }
  }

  private def handleSymmetryForAddLink(relation: Relation, tuple: Map[String, Any]) {
    if (relation.arguments.size == 2 && relation.sourceType == relation.destinationType.get)
    symmetry(relation) match {
      case Symmetric =>
        if (symmetryActions(relation) == Relation.Restore)
          addEdge(relation, Map((Relation.Source, tuple(Relation.Destination)),(Relation.Destination, tuple(Relation.Source))))
        else if (symmetryActions(relation) == Relation.Preserve)
          throw new WQueryUpdateBreaksRelationPropertyException("symmetric", relation)
      case Antisymmetric =>
        if (transitives(relation)) {
          if (reaches(relation, tuple(Relation.Destination), tuple(Relation.Source), Nil))
            throw new WQueryUpdateBreaksRelationPropertyException("transitive antisymmetry", relation)
        } else {
          if (follow(relation, Relation.Source, tuple(Relation.Destination), Relation.Destination).contains(tuple(Relation.Source))) {
            if (symmetryActions(relation) == Relation.Restore)
              removeMatchingLinks(relation, Map((Relation.Source, tuple(Relation.Destination)), (Relation.Destination, tuple(Relation.Source))))
            else if (symmetryActions(relation) == Relation.Preserve)
              throw new WQueryUpdateBreaksRelationPropertyException("antisymmetry", relation)
          }
        }
      case NonSymmetric =>
        // do nothing
    }
  }

  private def reaches(relation: Relation, source: Any, destination: Any, reached: List[Any]): Boolean = {
    val fringe = follow(relation, Relation.Source, source, Relation.Destination).filterNot(reached.contains(_))

    if (fringe.isEmpty) {
      false
    } else if (fringe.contains(destination)) {
      true
    } else {
      fringe.exists(newSource => reaches(relation, newSource, destination, reached :+ newSource))
    }
  }

  def removeSense(sense: Sense) {
    removeNode(SenseType, sense, true, true)
  }

  def removeSynset(synset: Synset) {
    removeNode(SynsetType, synset, true, true)
  }

  def removeWord(word: String) {
    removeNode(StringType, word, true, true)
  }

  def removePartOfSpeechSymbol(pos: String) {
    removeNode(POSType, pos, true, true)
  }

  private def removeNode(nodeType: NodeType, value: Any, withDependentNodes: Boolean, withCollectionDependentNodes: Boolean) = atomic {
    removeDependentLinks(nodeType, value, withDependentNodes, withCollectionDependentNodes)

    (nodeType: @unchecked) match {
      case SynsetType =>
        removeLink(WordNet.SynsetSet, Map((Relation.Source, value)))
      case SenseType =>
        removeLink(WordNet.SenseSet, Map((Relation.Source, value)))
      case StringType =>
        removeLink(WordNet.WordSet, Map((Relation.Source, value)))
      case POSType =>
        removeLink(WordNet.PosSet, Map((Relation.Source, value)))
    }
  }

  private def removeDependentLinks(nodeType: NodeType, obj: Any, withDependentNodes: Boolean, withCollectionDependentNodes: Boolean) {
    for (relation <- relations) {
      if (transitives(relation)) {
        if (transitivesActions(relation) == Relation.Restore) {
          for (source <- follow(relation, Relation.Destination, obj, Relation.Source);
               destination <- follow(relation, Relation.Source, obj, Relation.Destination)) {
            addEdge(relation, Map((Relation.Source, source), (Relation.Destination, destination)))
          }
        } else if (transitivesActions(relation) == Relation.Preserve && !follow(relation, Relation.Destination, obj, Relation.Source).isEmpty && !follow(relation, Relation.Source, obj, Relation.Destination).isEmpty) {
          throw new WQueryUpdateBreaksRelationPropertyException(Relation.Transitivity, relation)
        }
      }

      for (argument <- relation.arguments.filter(_.nodeType == nodeType))
        removeMatchingLinksByNode(relation, Map((argument.name, obj)), Some(obj), withDependentNodes, withCollectionDependentNodes)
    }
  }

  def setSynsets(synsets: List[Synset], assignments: List[PropertyAssignment]) {
    val (newSynsets, preservedSynsets) = synsets.partition(_.isInstanceOf[NewSynset])
    val preservedSynsetsSet = preservedSynsets.toSet
    val removedSynsets = fetch(WordNet.SynsetSet, List((Relation.Source, Nil)), List(Relation.Source)).paths.map(_.last.asInstanceOf[Synset]).filterNot(preservedSynsetsSet.contains(_))

    newSynsets.foreach(synset => addSynset(None, synset.asInstanceOf[NewSynset].senses, assignments))
    removedSynsets.foreach(removeSynset(_))
  }

  def setSenses(newSenses: List[Sense], assignments: List[PropertyAssignment]) = {
    setNodes[Sense](newSenses, WordNet.SenseSet, assignments, addSense, removeSense)
  }

  def setPartOfSpeechSymbols(newPartOfSpeechSymbols: List[String], assignments: List[PropertyAssignment]) = {
    setNodes[String](newPartOfSpeechSymbols, WordNet.PosSet, assignments, addPartOfSpeechSymbol, removePartOfSpeechSymbol)
  }

  def setWords(newWords: List[String], assignments: List[PropertyAssignment]) = {
    setNodes[String](newWords, WordNet.WordSet, assignments, addWord, removeWord)
  }

  private def setNodes[A](newNodes: List[A], nodesRelation: Relation, assignments: List[PropertyAssignment],
                  addFunction: ((A, List[PropertyAssignment]) => Unit),
                  removeFunction: (A => Unit)) {
    val nodesSet = fetch(nodesRelation, List((Relation.Source, Nil)), List(Relation.Source)).paths.map(_.last.asInstanceOf[A]).toSet
    val newNodesSet = newNodes.toSet

    for (node <- nodesSet if (!newNodesSet.contains(node)))
      removeFunction(node)

    for (node <- newNodesSet if (!nodesSet.contains(node)))
      addFunction(node, assignments)
  }

  def removeLink(relation: Relation, tuple: Map[String, Any], withDependentNodes: Boolean, withCollectionDependentNodes: Boolean) = atomic {
    removeLinkByNode(relation, tuple, None, withDependentNodes = withDependentNodes, withCollectionDependentNodes = withCollectionDependentNodes)
  }

  def removeLinkByNode(relation: Relation, tuple: Map[String, Any], node: Option[Any], symmetricEdge: Boolean = false,
                       withDependentNodes: Boolean = true, withCollectionDependentNodes: Boolean = true): Unit = atomic {
    val relationSuccessors = successors(relation)

    for ((sourceName, sourceValue) <- tuple if (relationSuccessors.contains(sourceName, sourceValue))) {
      val updatedSuccessors = relationSuccessors((sourceName, sourceValue)).filterNot(_ == tuple)

      handleRequiredByPropertyForRemoveLink(relation, sourceName, sourceValue, node, updatedSuccessors)
      relationSuccessors((sourceName, sourceValue)) = updatedSuccessors
    }

    if (withDependentNodes) {
      for (dependentArg <- dependent(relation))
        removeNode(relation.demandArgument(dependentArg).nodeType, tuple(dependentArg), true, true)
    }

    if (withCollectionDependentNodes) {
      for (collectionDependentArg <- collectionDependent(relation)) {
        val collectionDependentNode = tuple(collectionDependentArg)

        if (relationSuccessors(collectionDependentArg, collectionDependentNode).isEmpty)
          removeNode(relation.demandArgument(collectionDependentArg).nodeType, collectionDependentNode, true, true)
      }
    }

    handleSymmetryForRemoveLink(relation, tuple, node, symmetricEdge)
  }

  private def handleRequiredByPropertyForRemoveLink(relation: Relation, argumentName: String, argumentValue: Any, node: Option[Any], relationSuccessors: Seq[Map[String, Any]]) {
    if (requiredBys(relation).contains(argumentName) && node != Some(argumentValue)) {
      if (relationSuccessors.isEmpty)
        throw new WQueryUpdateBreaksRelationPropertyException(Relation.RequiredBy, relation, argumentName)
    }
  }

  private def handleSymmetryForRemoveLink(relation: Relation, tuple: Map[String, Any], node: Option[Any], symmetricEdge: Boolean) {
    if (relation.arguments.size == 2 && relation.sourceType == relation.destinationType.get && !symmetricEdge)
    symmetry(relation) match {
      case Symmetric =>
        removeLinkByNode(relation, Map((Relation.Source, tuple(Relation.Destination)),(Relation.Destination, tuple(Relation.Source))), node, symmetricEdge = true)
      case _ =>
        // do nothing
    }
  }

  def removeMatchingLinks(relation: Relation, tuple: Map[String, Any]) = atomic { removeMatchingLinksByNode(relation, tuple, None, true, true) }

  def removeMatchingLinksByNode(relation: Relation, tuple: Map[String, Any], node: Option[Any], withDependentNodes: Boolean, withCollectionDependentNodes: Boolean) = atomic {
    val relationSuccessors = successors(relation)

    for ((sourceName, sourceValue) <- tuple if (relationSuccessors.contains(sourceName, sourceValue))) {
      val matchingSuccessors = relationSuccessors((sourceName, sourceValue)).filter(succ => tuple.forall(elem => succ.exists(_ == elem)))

      matchingSuccessors.foreach(removeLinkByNode(relation, _, node, false, withDependentNodes, withCollectionDependentNodes))
    }
  }

  def setLinks(relation: Relation, sourceName: String, sourceValue: Any, tuples: Seq[Map[String, Any]]) = atomic {
    val relationSuccessors = successors(relation)
    val sourceSuccessors = relationSuccessors.get((sourceName, sourceValue)).getOrElse(TransactionalVector())
    val (matchingSuccessors, notMatchingSuccessors) = sourceSuccessors.partition(tuples.contains(_))

    notMatchingSuccessors.foreach(tuple => removeLinkByNode(relation, tuple, Some(sourceValue)))
    tuples.filter(!matchingSuccessors.contains(_)).foreach(addLink(relation, _))
  }


  private def demandSynset(sense: Sense) = {
    follow(WordNet.SenseToSynset, Relation.Source, sense, Relation.Destination).head.asInstanceOf[Synset]
  }

  def merge(synsets: List[Synset], senses: List[Sense], assignments: List[PropertyAssignment]) = atomic {
    if (!synsets.isEmpty || !senses.isEmpty) {
      val (destination, sources) = if (synsets.isEmpty) (Synset("synset#" + senses.head.toString), Nil) else (synsets.head, synsets.tail)
      val (addAssignments, setAssignments, removeAssignments) = partitionAssignments(assignments)
      val notSetRelations = relations.filter(relation => relation.name != "_" && !setAssignments.exists(_.pattern.relation.get == relation))
      val skippedTuples = removeAssignments.map(assignment => (assignment.pattern.relation.get, assignment.tuplesFor(destination))).toMap
      val destinationType = DataType.fromValue(destination)

      for (assignment <- removeAssignments; tuple <- assignment.tuplesFor(destination))
        removeMatchingLinksByNode(assignment.pattern.relation.get, tuple, None, true, true)

      for (synset <- sources; relation <- notSetRelations)
        copyLinks(relation, destinationType, synset, destination, skippedTuples.get(relation).getOrElse(Nil))

      for (sense <- senses; relation <- notSetRelations.filterNot(List(WordNet.SynsetToSenses, WordNet.SenseToSynset,WordNet.SynsetToWordForms, WordNet.WordFormToSynsets).contains(_))) {
        copyLinks(relation, destinationType, demandSynset(sense), destination, skippedTuples.get(relation).getOrElse(Nil))
      }

      for (assignment <- setAssignments) {
        removeMatchingLinksByNode(assignment.pattern.relation.get, Map((assignment.pattern.source.name, destination)), Some(destination), false, false)
        assignment.tuplesFor(destination).foreach(addLink(assignment.pattern.relation.get, _))
      }

      for (assignment <- addAssignments)
        assignment.tuplesFor(destination).foreach(addLink(assignment.pattern.relation.get, _))

      sources.foreach(removeNode(SynsetType, _, false, false))
      senses.foreach(sense => moveSense(sense, demandSynset(sense), destination))
    }
  }

  private def moveSense(sense: Sense, source: Synset, destination: Synset) {
    removeSuccessor(source, WordNet.SynsetToSenses, sense)
    removeSuccessor(source, WordNet.SynsetToWordForms, sense.wordForm)
    removeSuccessor(sense, WordNet.SenseToSynset, source, withCollectionDependentNodes = true)
    removeSuccessor(sense.wordForm, WordNet.WordFormToSynsets, source)
    addSuccessor(destination, WordNet.SynsetToSenses, sense)
    addSuccessor(destination, WordNet.SynsetToWordForms, sense.wordForm)
    addSuccessor(sense, WordNet.SenseToSynset, destination)
    addSuccessor(sense.wordForm, WordNet.WordFormToSynsets, destination)
  }

  private def copyLinks(relation: Relation, requiredType: DataType, source: Any, destination: Any, skippedLinks: List[Map[String, Any]]) {
    val relationSuccessors = successors(relation)

    for (argument <- relation.arguments if argument.nodeType == requiredType) {
      for (succs <- relationSuccessors.get((argument.name, source)); successor <- succs) {
        val tuple = ((argument.name, destination)::((successor - argument.name).toList)).toMap

        if (!skippedLinks.contains(tuple) && !containsLink(relation, tuple))
          addLink(relation, tuple)
      }
    }
  }

  private def partitionAssignments(assignments: List[PropertyAssignment]) = {
    val (addAssignments, rest) = assignments.partition(_.op == "+=")
    val (setAssignments, removeAssignments) = rest.partition(_.op == ":=")
    (addAssignments, setAssignments, removeAssignments)
  }

  def split(synsets: List[Synset], assignments: List[PropertyAssignment]) {
    for (synset <- synsets; sense <- getSenses(synset))
      merge(Nil, List(sense), assignments)
  }
}
