package org.wquery.model.impl

import collection.mutable.ListBuffer
import collection.mutable.{Map => MMap}
import org.wquery.model._
import org.wquery.utils.Cache
import org.wquery.{WQueryUpdateBreaksRelationPropertyException, WQueryModelException, WQueryEvaluationException}
import akka.stm._
import scalaz._
import Scalaz._
import org.wquery.engine.operations.NewSynset

class InMemoryWordNet extends WordNet {
  private val successors = TransactionalMap[Relation, TransactionalMap[(String, Any), IndexedSeq[Map[String, Any]]]]()
  private val aliasMap = scala.collection.mutable.Map[Relation, Arc]()
  private var relationsList = List[Relation]()
  private val statsCache = new Cache[WordNetStats](calculateStats, 1000)

  def relations = relationsList

  def aliases = aliasMap.keys.toList

  for (relation <- WordNet.Meta.relations)
    createRelation(relation)

  for (relation <- WordNet.relations)
    addRelation(relation)

  dependent ++= WordNet.dependent
  collectionDependent ++= WordNet.collectionDependent

  statsCache.invalidate

  def addRelation(relation: Relation) {
    if (!relations.contains(relation)) {
      relationsList = (relationsList :+ relation)
        .sortWith((l, r) => l.name < r.name || l.name == r.name && l.arguments.size < r.arguments.size ||
          l.name == r.name && l.arguments.size == r.arguments.size && l.sourceType < r.sourceType)
      createRelation(relation)
      dependent(relation) = ∅[Set[String]]
      collectionDependent(relation) = ∅[Set[String]]
      functionalFor(relation) = ∅[Set[String]]
      requiredBys(relation) = ∅[Set[String]]
      transitives(relation) = false
      symmetry(relation) = NonSymmetric
      statsCache.invalidate
    }
  }

  private def createRelation(relation: Relation) {
    successors(relation) = TransactionalMap[(String, Any), IndexedSeq[Map[String, Any]]]()
  }

  def removeRelation(relation: Relation) {
    if (WordNet.relations.contains(relation))
      throw new WQueryModelException("Cannot remove the mandatory relation '" + relation + "'")

    relationsList = relationsList.filterNot(_ == relation)
    successors.remove(relation)
    statsCache.invalidate
  }

  def setRelations(newRelations: List[Relation]) {
    if (WordNet.relations.exists(relation => !newRelations.contains(relation)))
      throw new WQueryModelException("Assignment removes a mandatory relation")

    for (relation <- relations if (!newRelations.contains(relation)))
      removeRelation(relation)

    for (relation <- newRelations if (!relations.contains(relation)))
      addRelation(relation)
  }

  private def follow(relation: Relation, from: String, through: Any, to: String) = {
    successors(relation).get((from, through)).map(_.map(succ => succ(to))).orZero
  }

  def getSenses(synset: Synset) = follow(WordNet.SynsetToSenses, Relation.Src, synset, Relation.Dst).toList.asInstanceOf[List[Sense]]

  def getSynset(sense: Sense) = {
    val synsets = follow(WordNet.SenseToSynset, Relation.Src, sense, Relation.Dst).toList

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

  private def appendDestinationTuples(destinationMaps: Seq[Map[String, Any]], destinations: List[(String, List[Any])],
                                      to: List[String], relation: Relation, buffer: ListBuffer[List[Any]]) {
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

  def fringe(fringeRelations: List[(Relation, String)], distinct: Boolean) = {
    val buffer = new DataSetBuffer

    for ((relation, argument) <- fringeRelations)
      buffer.append(fetch(relation, List((argument, Nil)), List(argument)))

    if (distinct) buffer.toDataSet.distinct else buffer.toDataSet
  }

  def extend(extensionSet: ExtensionSet, direction: Direction, through: (String, Option[NodeType]),
             to: List[(String, Option[NodeType])]): ExtendedExtensionSet = {
    val buffer = new ExtensionSetBuffer(extensionSet, direction)
    val toMap = to.toMap

    for (relation <- relations if relation.isTraversable;
         source <- relation.argumentNames if (through._1 == ArcPatternArgument.AnyName || through._1 == source) &&
            through._2.some(_ == relation.demandArgument(source).nodeType).none(true);
         destination <- relation.argumentNames if toMap.isEmpty ||
            toMap.get(destination)
              .some(nodeTypeOption => nodeTypeOption.some(_ == relation.demandArgument(destination).nodeType).none(true)).none(false)
         if source != destination)
      buffer.append(extendWithRelationTuples(extensionSet, relation, direction, source, List(destination)))

    buffer.toExtensionSet
  }

  def extend(extensionSet: ExtensionSet, relation: Relation, direction: Direction, through: String, to: List[String]) = {
    val buffer = new ExtensionSetBuffer(extensionSet, direction)

    if (aliasMap.contains(relation))
      extendWithAlias(extensionSet, relation, direction, through, to, buffer)
    else
      buffer.append(extendWithRelationTuples(extensionSet, relation, direction, through, to))

    buffer.toExtensionSet
  }

  private def extendWithRelationTuples(extensionSet: ExtensionSet, relation: Relation, direction: Direction, through: String, to: List[String]) = {
    relation.demandArgument(through)
    to.foreach(relation.demandArgument)

    direction match {
      case Forward =>
        extendWithRelationTuplesForward(extensionSet, relation, through, to)
      case Backward =>
        extendWithRelationTuplesBackward(extensionSet, relation, through, to)
    }
  }

  private def extendWithRelationTuplesForward(extensionSet: ExtensionSet, relation: Relation, through: String, to: List[String]) = {
    val relationSuccessors = successors(relation)
    val builder = new ExtensionSetBuilder(extensionSet, Forward)

    for (pathPos <- 0 until extensionSet.size) {
      val source = extensionSet.right(pathPos)

      for (relSuccs <- relationSuccessors.get((through, source)); succs <- relSuccs) {
        val extensionBuffer = new ListBuffer[Any]

        for(destination <- to if (succs.contains(destination))) {
          extensionBuffer.append(Arc(relation, through, destination))
          extensionBuffer.append(succs(destination))
        }

        val extension = extensionBuffer.toList

        if (!extension.isEmpty)
          builder.extend(pathPos, extension)
      }
    }

    builder.build
  }

  private def extendWithRelationTuplesBackward(extensionSet: ExtensionSet, relation: Relation, through: String, to: List[String]) = {
    val relationSuccessors = successors(relation)
    val builder = new ExtensionSetBuilder(extensionSet, Backward)

    for (pathPos <- 0 until extensionSet.size) {
      val sources = to.indices.map(index => extensionSet.left(pathPos, index*2))

      for (relSuccs <- relationSuccessors.get((to.head, sources.head)); succs <- relSuccs.distinct) {
        val extensionBuffer = new ListBuffer[Any]

        if (succs.contains(through) && (to zip sources).forall{ case (name, value) => succs(name) == value}) {
          extensionBuffer.append(succs(through))
          extensionBuffer.append(Arc(relation, through, to.head))
        }

        val extension = extensionBuffer.toList

        if (!extension.isEmpty)
          builder.extend(pathPos, extension)
      }
    }

    builder.build
  }

  private def extendWithAlias(extensionSet: ExtensionSet, relation: Relation, direction: Direction,
                                 through: String, to: List[String], buffer: ExtensionSetBuffer) {
    if (to.size == 1) {
      val arc = aliasMap(relation)

      (through, to.head) match {
        case (Relation.Src, Relation.Dst) =>
          buffer.append(extendWithRelationTuples(extensionSet, arc.relation, direction, arc.from, List(arc.to)))
        case (Relation.Dst, Relation.Src) =>
          buffer.append(extendWithRelationTuples(extensionSet, arc.relation, direction, arc.to, List(arc.from)))
        case _ =>
          throw new WQueryEvaluationException("One cannot traverse the alias " + relation + " using custom source or destination arguments")
      }
    } else {
      throw new WQueryEvaluationException("One cannot traverse the alias " + relation + " to multiple destinations")
    }
  }

  def stats = statsCache.get // stats are calculated below in calculateStats()

  private def calculateStats() = {
    val allRelations = relations ++ aliases ++ WordNet.Meta.relations

    val fetchAllMaxCounts = MMap[(Relation, String), BigInt](
      (for (relation <- allRelations; argument <- ArcPatternArgument.AnyName::relation.argumentNames) yield ((relation, argument), BigInt(0))): _*)
    val extendValueMaxCounts = MMap[(Relation, String), BigInt](
      (for (relation <- allRelations; argument <- ArcPatternArgument.AnyName::relation.argumentNames) yield ((relation, argument), BigInt(0))): _*)

    for ((relation, relationSuccessors) <- successors; ((argument, _), argumentSuccessors) <- relationSuccessors) {
      fetchAllMaxCounts((relation, argument)) += argumentSuccessors.size
      extendValueMaxCounts((relation, argument)) = extendValueMaxCounts((relation, argument)) max argumentSuccessors.size
    }

    for (((relation, argument), count) <- fetchAllMaxCounts if argument != ArcPatternArgument.AnyName)
      fetchAllMaxCounts((relation, ArcPatternArgument.AnyName)) += count

    for (((relation, argument), count) <- extendValueMaxCounts if argument != ArcPatternArgument.AnyName)
      extendValueMaxCounts((relation, ArcPatternArgument.AnyName)) += count

    // TODO provide precise statistics for patterns using RelationalPattern.fetchMaxCount etc.
    val fetchAnyRelationMaxCount = (for (((_, argument), count) <- fetchAllMaxCounts if argument == ArcPatternArgument.AnyName) yield count).sum
    val extendAnyRelationMaxCount = (for (((_, argument), count) <- extendValueMaxCounts if argument == ArcPatternArgument.AnyName) yield count).sum

    for ((relation, _) <- aliasMap; argument <- relation.argumentNames) {
      fetchAllMaxCounts((relation, argument)) = fetchAnyRelationMaxCount
      extendValueMaxCounts((relation, argument)) = extendAnyRelationMaxCount
    }

    new WordNetStats(relations, fetchAllMaxCounts.toMap, extendValueMaxCounts.toMap)
  }

  private def addSense(sense: Sense) {
    addSynset(None, List(sense))
  }

  def addSynset(synsetId: Option[String], senses: List[Sense], moveSenses: Boolean = true) = {
    val synset = new Synset(synsetId|("synset#" + senses.head))

    addNode(SynsetType, synset)
    addSuccessor(synset.id, WordNet.IdToSynset, synset)
    addSuccessor(synset, WordNet.SynsetToId, synset.id)

    for (sense <- senses) {
      if (moveSenses) {
        getSynset(sense)
          .some(synsetSense => moveSense(sense, synsetSense, synset))
          .none(createSense(sense, synset))
      } else {
        createSense(sense, synset)
      }
    }

    synset
  }

  private def createSense(sense: Sense, synset: Synset) {
    if (!isWord(sense.wordForm))
      addNode(StringType, sense.wordForm)

    if (!isPartOfSpeechSymbol(sense.pos))
      addNode(POSType, sense.pos)

    addNode(SenseType, sense)
    addSuccessor(sense, WordNet.SenseToWordForm, sense.wordForm)
    addSuccessor(sense, WordNet.SenseToSenseNumber, sense.senseNumber)
    addSuccessor(sense, WordNet.SenseToPos, sense.pos)
    addSuccessor(synset, WordNet.SynsetToSenses, sense)
    addSuccessor(synset, WordNet.SynsetToWordForms, sense.wordForm)
    addSuccessor(sense.wordForm, WordNet.WordFormToSenses, sense)
    addSuccessor(sense, WordNet.SenseToSynset, synset)
    addSuccessor(sense.wordForm, WordNet.WordFormToSynsets, synset)
    addLink(WordNet.SenseToWordFormSenseNumberAndPos,
      Map((Relation.Src, sense), (Relation.Dst, sense.wordForm), ("num", sense.senseNumber), ("pos", sense.pos)))
  }

  def addWord(word: String) { addNode(StringType, word) }

  def addPartOfSpeechSymbol(pos: String) { if (!isPartOfSpeechSymbol(pos)) addNode(POSType, pos) }

  private def isWord(word: String) = {
    successors(WordNet.WordSet).get(Relation.Src, word).some(!_.isEmpty).none(false)
  }

  private def isPartOfSpeechSymbol(pos: String) = {
    successors(WordNet.PosSet).get(Relation.Src, pos).some(!_.isEmpty).none(false)
  }

  private def addNode(nodeType: NodeType, node: Any) = atomic {
    val relation = WordNet.dataTypesRelations(nodeType)

    addLink(relation, Map((Relation.Src, node)))
  }

  def addTuple(relation: Relation, tuple: Map[String, Any]) = atomic {
    relation match {
      case WordNet.SynsetSet =>
        val synset = tuple(Relation.Src).asInstanceOf[NewSynset]
        addSynset(Some(synset.id), synset.senses)
      case WordNet.SenseSet =>
        addSense(tuple(Relation.Src).asInstanceOf[Sense])
      case WordNet.WordSet =>
        addWord(tuple(Relation.Src).asInstanceOf[String])
      case WordNet.PosSet =>
        addPartOfSpeechSymbol(tuple(Relation.Src).asInstanceOf[String])
      case WordNet.Meta.Aliases =>
        addAlias(tuple)
      case _ =>
        addLink(relation, tuple)
    }
  }

  def removeTuple(relation: Relation, tuple: Map[String, Any], withDependentNodes: Boolean = true,
                  withCollectionDependentNodes: Boolean = true) = atomic {
    relation match {
      case WordNet.SynsetSet =>
        removeSynset(tuple(Relation.Src).asInstanceOf[Synset])
      case WordNet.SenseSet =>
        removeSense(tuple(Relation.Src).asInstanceOf[Sense])
      case WordNet.WordSet =>
        removeWord(tuple(Relation.Src).asInstanceOf[String])
      case WordNet.PosSet =>
        removePartOfSpeechSymbol(tuple(Relation.Src).asInstanceOf[String])
      case WordNet.Meta.Aliases =>
        removeAlias(tuple)
      case _ =>
        removeLink(relation, tuple, withDependentNodes, withCollectionDependentNodes)
    }
  }

  private def addAlias(tuple: Map[String, Any]) {
    val (alias, arc) = createAliasFromTuple(tuple)
    aliasMap.put(alias, arc)
    addEdge(WordNet.Meta.Aliases, tuple)
    statsCache.invalidate
  }

  private def removeAlias(tuple: Map[String, Any]) {
    val (alias, _) = createAliasFromTuple(tuple)
    aliasMap.remove(alias)
    removeEdge(WordNet.Meta.Aliases, tuple)
    statsCache.invalidate
  }

  private def createAliasFromTuple(tuple: Map[String, Any]) = {
    // TODO implement validations
    val relationName = tuple(WordNet.Meta.Aliases.Relation).asInstanceOf[String]
    val sourceName = tuple(WordNet.Meta.Aliases.Source).asInstanceOf[String]
    val destinationName = tuple(WordNet.Meta.Aliases.Destination).asInstanceOf[String]
    val aliasName = tuple(WordNet.Meta.Aliases.Name).asInstanceOf[String]
    val relation = schema.demandRelation(relationName, Map((sourceName, DataType.all), (destinationName, DataType.all)))
    val sourceType = relation.demandArgument(sourceName).nodeType
    val destinationType = relation.demandArgument(destinationName).nodeType
    val alias = Relation.binary(aliasName, sourceType, destinationType)
    val arc = Arc(relation, sourceName, destinationName)

    (alias, arc)
  }

  def setTuples(relation: Relation, sourceNames: List[String], sources: List[List[Any]],
                destinationNames: List[String], destinations: List[List[Any]]) = atomic {
    relation match {
      case WordNet.SynsetSet =>
        setSynsets(destinations.map(_.head.asInstanceOf[Synset]))
      case WordNet.SenseSet =>
        setSenses(destinations.map(_.head.asInstanceOf[Sense]))
      case WordNet.WordSet =>
        setWords(destinations.map(_.head.asInstanceOf[String]))
      case WordNet.PosSet =>
        setPartOfSpeechSymbols(destinations.map(_.head.asInstanceOf[String]))
      case WordNet.Meta.Relations =>

      case _ =>
        setLinks(relation, sourceNames, sources, destinationNames, destinations)
    }
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

    statsCache.age
  }

  private def removeEdge(relation: Relation, tuple: Map[String, Any]) = atomic {
    val relationSuccessors = successors(relation)

    for ((sourceName, sourceValue) <- tuple if relationSuccessors.contains((sourceName, sourceValue))) {
      relationSuccessors((sourceName, sourceValue)) = relationSuccessors((sourceName, sourceValue)).filterNot(_ == tuple)
    }

    statsCache.age
  }

  private def containsLink(relation: Relation, tuple: Map[String, Any]) = {
    successors(relation).get(Relation.Src, tuple(Relation.Src)).some(_.contains(tuple)).none(false)
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
          addEdge(relation, Map((Relation.Src, tuple(Relation.Dst)),(Relation.Dst, tuple(Relation.Src))))
        else if (symmetryActions(relation) == Relation.Preserve)
          throw new WQueryUpdateBreaksRelationPropertyException("symmetric", relation)
      case Antisymmetric =>
        if (transitives(relation)) {
          if (reaches(relation, tuple(Relation.Dst), tuple(Relation.Src), Nil))
            throw new WQueryUpdateBreaksRelationPropertyException("transitive antisymmetry", relation)
        } else {
          if (follow(relation, Relation.Src, tuple(Relation.Dst), Relation.Dst).contains(tuple(Relation.Src))) {
            if (symmetryActions(relation) == Relation.Restore)
              removeMatchingLinks(relation, Map((Relation.Src, tuple(Relation.Dst)), (Relation.Dst, tuple(Relation.Src))))
            else if (symmetryActions(relation) == Relation.Preserve)
              throw new WQueryUpdateBreaksRelationPropertyException("antisymmetry", relation)
          }
        }
      case NonSymmetric =>
        // do nothing
    }
  }

  private def reaches(relation: Relation, source: Any, destination: Any, reached: List[Any]): Boolean = {
    val fringe = follow(relation, Relation.Src, source, Relation.Dst).filterNot(reached.contains(_))

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

  private def removeNode(domainType: DomainType, value: Any, withDependentNodes: Boolean, withCollectionDependentNodes: Boolean) = atomic {
    removeDependentLinks(domainType, value, withDependentNodes, withCollectionDependentNodes)
    removeLink(WordNet.dataTypesRelations(domainType), Map((Relation.Src, value)))
  }

  private def removeDependentLinks(nodeType: NodeType, obj: Any, withDependentNodes: Boolean, withCollectionDependentNodes: Boolean) {
    for (relation <- relations) {
      if (transitives(relation)) {
        if (transitivesActions(relation) == Relation.Restore) {
          for (source <- follow(relation, Relation.Dst, obj, Relation.Src);
               destination <- follow(relation, Relation.Src, obj, Relation.Dst)) {
            addEdge(relation, Map((Relation.Src, source), (Relation.Dst, destination)))
          }
        } else if (transitivesActions(relation) == Relation.Preserve &&
            !follow(relation, Relation.Dst, obj, Relation.Src).isEmpty &&
            !follow(relation, Relation.Src, obj, Relation.Dst).isEmpty) {
          throw new WQueryUpdateBreaksRelationPropertyException(Relation.Transitivity, relation)
        }
      }

      for (argument <- relation.arguments.filter(_.nodeType == nodeType))
        removeMatchingLinksByNode(relation, Map((argument.name, obj)), Some(obj), withDependentNodes, withCollectionDependentNodes)
    }
  }

  def setSynsets(synsets: List[Synset]) {
    val (newSynsets, preservedSynsets) = synsets.partition(_.isInstanceOf[NewSynset])
    val preservedSynsetsSet = preservedSynsets.toSet
    val removedSynsets = fetch(WordNet.SynsetSet, List((Relation.Src, Nil)),
      List(Relation.Src)).paths.map(_.last.asInstanceOf[Synset]).filterNot(preservedSynsetsSet.contains(_))

    newSynsets.foreach(synset => addSynset(None, synset.asInstanceOf[NewSynset].senses))
    removedSynsets.foreach(removeSynset(_))
  }

  def setSenses(newSenses: List[Sense]) = {
    setNodes[Sense](newSenses, WordNet.SenseSet, addSense, removeSense)
  }

  def setPartOfSpeechSymbols(newPartOfSpeechSymbols: List[String]) = {
    setNodes[String](newPartOfSpeechSymbols, WordNet.PosSet, addPartOfSpeechSymbol, removePartOfSpeechSymbol)
  }

  def setWords(newWords: List[String]) = {
    setNodes[String](newWords, WordNet.WordSet, addWord, removeWord)
  }

  private def setNodes[A](newNodes: List[A], nodesRelation: Relation,
                  addFunction: (A => Unit),
                  removeFunction: (A => Unit)) {
    val nodesSet = fetch(nodesRelation, List((Relation.Src, Nil)), List(Relation.Src)).paths.map(_.last.asInstanceOf[A]).toSet
    val newNodesSet = newNodes.toSet

    for (node <- nodesSet if (!newNodesSet.contains(node)))
      removeFunction(node)

    for (node <- newNodesSet if (!nodesSet.contains(node)))
      addFunction(node)
  }

  def removeLink(relation: Relation, tuple: Map[String, Any], withDependentNodes: Boolean = true, withCollectionDependentNodes: Boolean = true) = atomic {
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
      for (dependentArg <- dependent(relation) if DataType.domain.contains(relation.demandArgument(dependentArg).nodeType))
        removeNode(relation.demandArgument(dependentArg).nodeType.asInstanceOf[DomainType], tuple(dependentArg), true, true)
    }

    if (withCollectionDependentNodes) {
      for (collectionDependentArg <- collectionDependent(relation) if DataType.domain.contains(relation.demandArgument(collectionDependentArg).nodeType)) {
        val collectionDependentNode = tuple(collectionDependentArg)

        if (relationSuccessors(collectionDependentArg, collectionDependentNode).isEmpty)
          removeNode(relation.demandArgument(collectionDependentArg).nodeType.asInstanceOf[DomainType], collectionDependentNode, true, true)
      }
    }

    handleSymmetryForRemoveLink(relation, tuple, node, symmetricEdge)
    statsCache.age
  }

  private def handleRequiredByPropertyForRemoveLink(relation: Relation, argumentName: String, argumentValue: Any,
                                                    node: Option[Any], relationSuccessors: Seq[Map[String, Any]]) {
    if (requiredBys(relation).contains(argumentName) && node != Some(argumentValue)) {
      if (relationSuccessors.isEmpty)
        throw new WQueryUpdateBreaksRelationPropertyException(Relation.RequiredBy, relation, argumentName)
    }
  }

  private def handleSymmetryForRemoveLink(relation: Relation, tuple: Map[String, Any], node: Option[Any], symmetricEdge: Boolean) {
    if (relation.arguments.size == 2 && relation.sourceType == relation.destinationType.get && !symmetricEdge)
    symmetry(relation) match {
      case Symmetric =>
        removeLinkByNode(relation,
          Map((Relation.Src, tuple(Relation.Dst)),(Relation.Dst, tuple(Relation.Src))), node, symmetricEdge = true)
      case _ =>
        // do nothing
    }
  }

  def removeMatchingLinks(relation: Relation, tuple: Map[String, Any]) = atomic { removeMatchingLinksByNode(relation, tuple, None, true, true) }

  def removeMatchingLinksByNode(relation: Relation, tuple: Map[String, Any], node: Option[Any],
                                withDependentNodes: Boolean, withCollectionDependentNodes: Boolean) = atomic {
    val relationSuccessors = successors(relation)

    for ((sourceName, sourceValue) <- tuple if (relationSuccessors.contains(sourceName, sourceValue))) {
      val matchingSuccessors = relationSuccessors((sourceName, sourceValue)).filter(succ => tuple.forall(elem => succ.exists(_ == elem)))

      matchingSuccessors.foreach(removeLinkByNode(relation, _, node, false, withDependentNodes, withCollectionDependentNodes))
    }
  }

  def setLinks(relation: Relation, sourceNames: List[String], sources: List[List[Any]],
               destinationNames: List[String], destinations: List[List[Any]]) = atomic {
    if (sourceNames.isEmpty)
      replaceLinks(relation, destinationNames, destinations)
    else
      replaceLinksBySource(relation, sources, sourceNames, destinationNames, destinations)
  }

  def replaceLinks(relation: Relation, destinationNames: List[String], destinations: List[List[Any]]) {
    successors(relation).clear()

    for (destination <- destinations)
      addLink(relation, destinationNames.zip(destination).toMap)
  }

  def replaceLinksBySource(relation: Relation, sources: List[List[Any]], sourceNames: List[String],
                           destinationNames: List[String], destinations: List[List[Any]]) {
    val relationSuccessors = successors(relation)

    for (source <- sources; i <- 0 until sourceNames.size) {
      val sourceName = sourceNames(i)
      val sourceValue = source(i)
      val sourceSuccessors = relationSuccessors.getOrElse((sourceName, sourceValue), TransactionalVector())
      val (matchingSuccessors, _) = sourceSuccessors.partition({
        successor =>
          sourceNames.zip(source).forall {
            case (name, value) => successor(name) == value
          }
      })

      matchingSuccessors.foreach(tuple => removeLinkByNode(relation, tuple, Some(sourceValue)))

      for (destination <- destinations) {
        val tuple = (sourceNames.zip(source) ++ destinationNames.zip(destination)).toMap
        addLink(relation, tuple)
      }
    }
  }

  private def demandSynset(sense: Sense) = {
    follow(WordNet.SenseToSynset, Relation.Src, sense, Relation.Dst).head.asInstanceOf[Synset]
  }

  def merge(synsets: List[Synset], senses: List[Sense]) = atomic {
    if (!synsets.isEmpty || !senses.isEmpty) {
      val (destination, sources) = if (synsets.isEmpty) (Synset("synset#" + senses.head.toString), Nil) else (synsets.head, synsets.tail)
      val notAnyRelations = relations.filter(relation => relation.name != Relation.AnyName)
      val destinationType = DataType.fromValue(destination)

      for (synset <- sources; relation <- notAnyRelations)
        copyLinks(relation, destinationType, synset, destination)

      for (sense <- senses; relation <- notAnyRelations
            .filterNot(List(WordNet.SynsetToSenses, WordNet.SenseToSynset,WordNet.SynsetToWordForms, WordNet.WordFormToSynsets).contains(_))) {
        copyLinks(relation, destinationType, demandSynset(sense), destination)
      }

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

  private def copyLinks(relation: Relation, requiredType: DataType, source: Any, destination: Any) {
    val relationSuccessors = successors(relation)

    for (argument <- relation.arguments if argument.nodeType == requiredType) {
      for (succs <- relationSuccessors.get((argument.name, source)); successor <- succs) {
        val tuple = ((argument.name, destination)::((successor - argument.name).toList)).toMap

        if (!containsLink(relation, tuple))
          addLink(relation, tuple)
      }
    }
  }
}
