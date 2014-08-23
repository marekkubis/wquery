package org.wquery.model.impl

import org.wquery.model._
import org.wquery.path.operations.NewSynset
import org.wquery.{WQueryModelException, WQueryUpdateBreaksRelationPropertyException}

import scala.collection.mutable.{ListBuffer, Map => MMap}
import scala.concurrent.stm._
import scalaz.Scalaz._

class InMemoryWordNet extends WordNet {
  private val store = new InMemoryWordNetStore(TMap())
  private val aliasMap = scala.collection.mutable.Map[Relation, List[Arc]]()
  private var relationsList = List[Relation]()

  // relation properties
  private val dependent = MMap[Relation, Set[String]]()
  private val collectionDependent = MMap[Relation, Set[String]]()
  private val functionalFor = MMap[Relation, Set[String]]()
  private val requiredBys = MMap[Relation, Set[String]]()
  private val transitives = MMap[Relation, List[(String, String)]]()
  private val symmetry = MMap[Relation, MMap[(String, String), Symmetry]]()

  private val transitivesActions = MMap[(Relation, String, String), String]()
  private val symmetryActions = MMap[(Relation, String, String), String]()

  def relations = relationsList

  def aliases = aliasMap.keys.toList

  for (relation <- WordNet.Meta.relations)
    createRelation(relation)

  for (relation <- WordNet.relations)
    addRelation(relation)

  dependent ++= WordNet.dependent
  collectionDependent ++= WordNet.collectionDependent

  def addRelation(relation: Relation) {
    if (!relations.contains(relation)) {
      relationsList = (relationsList :+ relation)
        .sortWith((l, r) => l.name < r.name || l.name == r.name && l.arguments.size < r.arguments.size ||
          l.name == r.name && l.arguments.size == r.arguments.size && l.sourceType < r.sourceType)
      createRelation(relation)
    }
  }

  private def createRelation(relation: Relation) = atomic { implicit txn =>
    store.successors(relation) = TMap[(String, Any), Vector[Map[String, Any]]]()
  }

  def removeRelation(relation: Relation) = atomic { implicit txn =>
    if (WordNet.relations.contains(relation))
      throw new WQueryModelException("Cannot remove the mandatory relation '" + relation + "'")

    relationsList = relationsList.filterNot(_ == relation)
    store.successors.remove(relation)
  }

  def setRelations(newRelations: List[Relation]) {
    if (WordNet.relations.exists(relation => !newRelations.contains(relation)))
      throw new WQueryModelException("Assignment removes a mandatory relation")

    for (relation <- relations if (!newRelations.contains(relation)))
      removeRelation(relation)

    for (relation <- newRelations if (!relations.contains(relation)))
      addRelation(relation)
  }

  private def follow(relation: Relation, from: String, through: Any, to: String) = atomic { implicit txn =>
    store.successors(relation).get((from, through)).map(_.map(succ => succ(to))).orZero
  }

  def getSenses(synset: Synset) = follow(WordNet.SynsetToSenses, Relation.Src, synset, Relation.Dst).toList.asInstanceOf[List[Sense]]

  def getSenses(word: String) = follow(WordNet.WordFormToSenses, Relation.Src, word, Relation.Dst).toList.asInstanceOf[List[Sense]]

  def getSynset(sense: Sense) = {
    val synsets = follow(WordNet.SenseToSynset, Relation.Src, sense, Relation.Dst).toList

    if (synsets.isEmpty) None else Some(synsets.head.asInstanceOf[Synset])
  }

  def fetch(relation: Relation, from: List[(String, List[Any])], to: List[String], withArcs: Boolean) = atomic { implicit txn =>
    val buffer = DataSetBuffers.createPathBuffer
    val (sourceName, sourceValues) = from.head
    val destinations = from.tail

    if (sourceValues.isEmpty) {
      for (((src, obj), destinationMaps) <- store.successors(relation) if (src == sourceName))
        appendDestinationTuples(destinationMaps, destinations, to, relation, buffer, withArcs)
    } else {
      for (sourceValue <- sourceValues)
        store.successors(relation).get((sourceName, sourceValue))
          .map(appendDestinationTuples(_, destinations, to, relation, buffer, withArcs))
    }

    DataSet(buffer.toList)
  }

  private def appendDestinationTuples(destinationMaps: Seq[Map[String, Any]], destinations: List[(String, List[Any])],
                                      to: List[String], relation: Relation, buffer: ListBuffer[List[Any]], withArcs: Boolean) {
    for (destinationMap <- destinationMaps) {
      if (destinations.forall(dest => destinationMap.contains(dest._1) && (dest._2.isEmpty || dest._2.contains(destinationMap(dest._1))))) {
        val tupleBuffer = new ListBuffer[Any]

        tupleBuffer.append(destinationMap(to.head))

        for (destinationName <- to.tail) {
          if (withArcs)
            tupleBuffer.append(Arc(relation, to.head, destinationName))

          tupleBuffer.append(destinationMap(destinationName))
        }

        buffer.append(tupleBuffer.toList)
      }
    }
  }

  def extend(extensionSet: ExtensionSet, srcType: Option[NodeType],
             dstType: Option[NodeType], inverted: Boolean): ExtendedExtensionSet = {
    val buffer = new ExtensionSetBuffer(extensionSet)
    val (srcName, dstName) = if (inverted) (Relation.Dst, Relation.Src) else (Relation.Src, Relation.Dst)
    val toMap = Map((dstName, dstType))

    for (relation <- relations if relation.isTraversable && srcType.some(_ == relation.demandArgument(srcName).nodeType).none(true)
         if toMap.get(dstName).some(nodeTypeOption => nodeTypeOption.some(_ == relation.demandArgument(dstName).nodeType)
        .none(true)).none(false))
      buffer.append(extendWithRelationTuples(extensionSet, relation, inverted))

    buffer.toExtensionSet
  }

  def extend(extensionSet: ExtensionSet, relation: Relation, inverted: Boolean) = {
    val buffer = new ExtensionSetBuffer(extensionSet)

    if (aliasMap.contains(relation))
      extendWithAlias(extensionSet, relation, inverted, buffer)
    else
      buffer.append(extendWithRelationTuples(extensionSet, relation, inverted))

    buffer.toExtensionSet
  }

  private def extendWithRelationTuples(extensionSet: ExtensionSet, relation: Relation, inverted: Boolean) = atomic { implicit txn =>
    val relationSuccessors = store.successors(relation)
    val builder = new ExtensionSetBuilder(extensionSet)

    val (through, to) = if (inverted)
      (Relation.Dst, Relation.Src)
    else
      (Relation.Src, Relation.Dst)

    for (pathPos <- 0 until extensionSet.size) {
      val source = extensionSet.right(pathPos)

      for (relSuccs <- relationSuccessors.get((through, source)); succs <- relSuccs) {
        val extensionBuffer = new ListBuffer[Any]

        if (succs.contains(to)) {
          extensionBuffer.append(Arc(relation, through, to))
          extensionBuffer.append(succs(to))
        }

        val extension = extensionBuffer.toList

        if (!extension.isEmpty)
          builder.extend(pathPos, extension)
      }
    }

    builder.build
  }

  private def extendWithAlias(extensionSet: ExtensionSet, relation: Relation,
                                 inverted: Boolean, buffer: ExtensionSetBuffer) {
    val arcs = aliasMap(relation)

    for (arc <- arcs) {
      val relationInverted = (arc.from == Relation.Src && arc.to == Relation.Dst && inverted
        || arc.from == Relation.Dst && arc.to == Relation.Src && !inverted)
      buffer.append(extendWithRelationTuples(extensionSet, arc.relation, relationInverted))
    }
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

  private def isWord(word: String) = atomic { implicit txn =>
    store.successors(WordNet.WordSet).get(Relation.Src, word).some(!_.isEmpty).none(false)
  }

  private def isPartOfSpeechSymbol(pos: String) = atomic { implicit txn =>
    store.successors(WordNet.PosSet).get(Relation.Src, pos).some(!_.isEmpty).none(false)
  }

  private def addNode(nodeType: NodeType, node: Any) = atomic { implicit txn =>
    val relation = WordNet.dataTypesRelations(nodeType)

    addLink(relation, Map((Relation.Src, node)))
  }

  def addTuple(relation: Relation, tuple: Map[String, Any]) = atomic { implicit txn =>
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
      case WordNet.Meta.Relations =>
        addMetaRelation(tuple)
      case WordNet.Meta.Arguments =>
        addMetaArgument(tuple)
      case WordNet.Meta.Properties =>
        addMetaProperty(tuple)
      case WordNet.Meta.PairProperties =>
        addMetaPairProperty(tuple)
      case WordNet.Meta.Dependencies =>
        addMetaDependency(tuple)
      case WordNet.Meta.Aliases =>
        addMetaAlias(tuple)
      case _ =>
        addLink(relation, tuple)
    }
  }

  def removeTuple(relation: Relation, tuple: Map[String, Any], withDependentNodes: Boolean = true,
                  withCollectionDependentNodes: Boolean = true) = atomic { implicit txn =>
    relation match {
      case WordNet.SynsetSet =>
        removeSynset(tuple(Relation.Src).asInstanceOf[Synset])
      case WordNet.SenseSet =>
        removeSense(tuple(Relation.Src).asInstanceOf[Sense])
      case WordNet.WordSet =>
        removeWord(tuple(Relation.Src).asInstanceOf[String])
      case WordNet.PosSet =>
        removePartOfSpeechSymbol(tuple(Relation.Src).asInstanceOf[String])
      case WordNet.Meta.Relations =>
        removeMetaRelation(tuple)
      case WordNet.Meta.Arguments =>
        removeMetaArgument(tuple)
      case WordNet.Meta.Properties =>
        removeMetaProperty(tuple)
      case WordNet.Meta.PairProperties =>
        removeMetaPairProperty(tuple)
      case WordNet.Meta.Dependencies =>
        removeMetaDependency(tuple)
      case WordNet.Meta.Aliases =>
        removeMetaAlias(tuple)
      case _ =>
        removeLink(relation, tuple, withDependentNodes, withCollectionDependentNodes)
    }
  }

  private def addMetaRelation(tuple: Map[String, Any]) {
    // TODO implement validations
    addEdge(WordNet.Meta.Relations, tuple)
  }

  private def removeMetaRelation(tuple: Map[String, Any]) {
    // TODO implement validations
    val relationName = tuple(WordNet.Meta.Relations.Name).asInstanceOf[String]
    val relation = schema.demandRelation(relationName, Map())

    removeEdge(WordNet.Meta.Relations, tuple)
    removeRelation(relation)
  }

  private def createRelationArgumentFromTuple(tuple: Map[String, Any]) = {
    // TODO implement validations
    val relationName = tuple(WordNet.Meta.Arguments.Relation).asInstanceOf[String]
    val argumentName = tuple(WordNet.Meta.Arguments.Argument).asInstanceOf[String]
    val argumentType = NodeType.fromName(tuple(WordNet.Meta.Arguments.Type).asInstanceOf[String])
    val argumentPosition = tuple(WordNet.Meta.Arguments.Position).asInstanceOf[Int]

    (relationName, Argument(argumentName, argumentType), argumentPosition)
  }

  private def addMetaArgument(tuple: Map[String, Any]) {
    val (relationName, argument, pos) = createRelationArgumentFromTuple(tuple)
    val relation = schema.getRelation(relationName, Map())
      .some { oldRelation =>
      removeRelation(oldRelation)
      val (leftArguments, rightArguments) = oldRelation.arguments.splitAt(pos)
      Relation(relationName, leftArguments ++ (argument::rightArguments))
    } .none {
      Relation(relationName, List(argument))
    }

    addRelation(relation)
    addEdge(WordNet.Meta.Arguments, tuple)
  }

  private def removeMetaArgument(tuple: Map[String, Any]) {
    val (relationName, argument, _) = createRelationArgumentFromTuple(tuple)
    val oldRelation = schema.demandRelation(relationName, Map((argument.name, Set(argument.nodeType))))

    removeRelation(oldRelation)
    addRelation(Relation(relationName, oldRelation.arguments.filterNot(_ == argument)))
    removeEdge(WordNet.Meta.Arguments, tuple)
  }

  private def addMetaProperty(tuple: Map[String, Any]) {
    // TODO implement validations
    val relationName = tuple(WordNet.Meta.Properties.Relation).asInstanceOf[String]
    val argumentName = tuple(WordNet.Meta.Properties.Argument).asInstanceOf[String]
    val property = tuple(WordNet.Meta.Properties.Property).asInstanceOf[String]
    val relation = schema.demandRelation(relationName, Map())

    property match {
      case WordNet.Meta.Properties.PropertyValueFunctional =>
        if (!functionalFor.contains(relation))
          functionalFor(relation) = Set(argumentName)
        else
          functionalFor(relation) = functionalFor(relation) + argumentName
      case WordNet.Meta.Properties.PropertyValueRequired =>
        if (!requiredBys.contains(relation))
          requiredBys(relation) = Set(argumentName)
        else
          requiredBys(relation) = requiredBys(relation) + argumentName
      case _ =>
        throw new WQueryModelException("Unknown relation argument property " + property)
    }

    addEdge(WordNet.Meta.Properties, tuple)
  }

  private def removeMetaProperty(tuple: Map[String, Any]) {
    // TODO implement validations
    val relationName = tuple(WordNet.Meta.Properties.Relation).asInstanceOf[String]
    val argumentName = tuple(WordNet.Meta.Properties.Argument).asInstanceOf[String]
    val property = tuple(WordNet.Meta.Properties.Property).asInstanceOf[String]
    val relation = schema.demandRelation(relationName, Map())

    property match {
      case WordNet.Meta.Properties.PropertyValueFunctional =>
        if (functionalFor.contains(relation))
          functionalFor(relation) = functionalFor(relation) - argumentName
      case WordNet.Meta.Properties.PropertyValueRequired =>
        if (requiredBys.contains(relation))
          requiredBys(relation) = requiredBys(relation) - argumentName
      case _ =>
        throw new WQueryModelException("Unknown relation argument property " + property)
    }

    removeEdge(WordNet.Meta.Properties, tuple)
  }

  private def addMetaPairProperty(tuple: Map[String, Any]) {
    // TODO implement validations
    val relationName = tuple(WordNet.Meta.PairProperties.Relation).asInstanceOf[String]
    val source = tuple(WordNet.Meta.PairProperties.Source).asInstanceOf[String]
    val destination = tuple(WordNet.Meta.PairProperties.Destination).asInstanceOf[String]
    val property = tuple(WordNet.Meta.PairProperties.Property).asInstanceOf[String]
    val action = tuple(WordNet.Meta.PairProperties.Action).asInstanceOf[String]
    val relation = schema.demandRelation(relationName, Map())

    property match {
      case WordNet.Meta.PairProperties.PropertyValueSymmetric =>
        if (!symmetry.contains(relation))
          symmetry.put(relation, MMap())

        symmetry(relation).put((source, destination), Symmetric)
        symmetryActions.put((relation, source, destination), action)
      case WordNet.Meta.PairProperties.PropertyValueAntisymmetric =>
        if (!symmetry.contains(relation))
          symmetry.put(relation, MMap())

        symmetry(relation).put((source, destination), Antisymmetric)
        symmetryActions.put((relation, source, destination), action)
      case WordNet.Meta.PairProperties.PropertyValueNonSymmetric =>
        if (!symmetry.contains(relation))
          symmetry.put(relation, MMap())

        symmetry(relation).put((source, destination), NonSymmetric)
        symmetryActions.put((relation, source, destination), action)
      case WordNet.Meta.PairProperties.PropertyValueTransitive =>
        if (!transitives.contains(relation))
          transitives.put(relation, Nil)

        transitives(relation) = (source, destination)::transitives(relation)
        transitivesActions.put((relation, source, destination), action)
      case _ =>
        throw new WQueryModelException("Unknown relation argument pair property " + property)
    }

    addEdge(WordNet.Meta.PairProperties, tuple)
  }

  private def removeMetaPairProperty(tuple: Map[String, Any]) {
    // TODO implement validations
    val relationName = tuple(WordNet.Meta.PairProperties.Relation).asInstanceOf[String]
    val source = tuple(WordNet.Meta.PairProperties.Source).asInstanceOf[String]
    val destination = tuple(WordNet.Meta.PairProperties.Destination).asInstanceOf[String]
    val property = tuple(WordNet.Meta.PairProperties.Property).asInstanceOf[String]
    val relation = schema.demandRelation(relationName, Map())

    property match {
      case WordNet.Meta.PairProperties.PropertyValueSymmetric =>
        removeSymmetryIfEqual(relation, source, destination, Symmetric)
      case WordNet.Meta.PairProperties.PropertyValueAntisymmetric =>
        removeSymmetryIfEqual(relation, source, destination, Antisymmetric)
      case WordNet.Meta.PairProperties.PropertyValueNonSymmetric =>
        removeSymmetryIfEqual(relation, source, destination, NonSymmetric)
      case WordNet.Meta.PairProperties.PropertyValueTransitive =>
        if (transitives.contains(relation)) {
          transitives(relation) = transitives(relation).filterNot(_ == (source, destination))
          transitivesActions.remove((relation, source, destination))
        }
      case _ =>
        throw new WQueryModelException("Unknown relation argument pair property " + property)
    }

    removeEdge(WordNet.Meta.PairProperties, tuple)
  }

  private def removeSymmetryIfEqual(relation: Relation, source: String, destination: String, symmetryValue: Symmetry) = {
    symmetry.get(relation).map { symmetries =>
      symmetries.get((source, destination)).map{ sym =>
        if (sym == symmetryValue) {
          symmetries.remove((source, destination))
          symmetryActions.remove((relation, source, destination))
        }
      }
    }
  }

  private def addMetaDependency(tuple: Map[String, Any]) {
    // TODO implement validations
    val relationName = tuple(WordNet.Meta.Dependencies.Relation).asInstanceOf[String]
    val argumentName = tuple(WordNet.Meta.Dependencies.Argument).asInstanceOf[String]
    val typeName = tuple(WordNet.Meta.Dependencies.Type).asInstanceOf[String]
    val relation = schema.demandRelation(relationName, Map())

    typeName match {
      case WordNet.Meta.Dependencies.TypeValueMember =>
        if (!dependent.contains(relation))
          dependent(relation) = Set(argumentName)
        else
          dependent(relation) = dependent(relation) + argumentName
      case WordNet.Meta.Dependencies.TypeValueSet =>
        if (!collectionDependent.contains(relation))
          collectionDependent(relation) = Set(argumentName)
        else
          collectionDependent(relation) = collectionDependent(relation) + argumentName
      case _ =>
        throw new WQueryModelException("Unknown relation dependency type " + typeName)
    }

    addEdge(WordNet.Meta.Dependencies, tuple)
  }

  private def removeMetaDependency(tuple: Map[String, Any]) {
    // TODO implement validations
    val relationName = tuple(WordNet.Meta.Dependencies.Relation).asInstanceOf[String]
    val argumentName = tuple(WordNet.Meta.Dependencies.Argument).asInstanceOf[String]
    val typeName = tuple(WordNet.Meta.Dependencies.Type).asInstanceOf[String]
    val relation = schema.demandRelation(relationName, Map())

    typeName match {
      case WordNet.Meta.Dependencies.TypeValueMember =>
        if (dependent.contains(relation))
          dependent(relation) = dependent(relation) - argumentName
      case WordNet.Meta.Dependencies.TypeValueSet =>
        if (collectionDependent.contains(relation))
          collectionDependent(relation) = collectionDependent(relation) - argumentName
      case _ =>
        throw new WQueryModelException("Unknown relation dependency type " + typeName)
    }

    removeEdge(WordNet.Meta.Dependencies, tuple)
  }

  private def addMetaAlias(tuple: Map[String, Any]) {
    val (alias, arc) = createAliasFromTuple(tuple)
    aliasMap.put(alias, arc::(~aliasMap.get(alias)))
    addEdge(WordNet.Meta.Aliases, tuple)
  }

  private def removeMetaAlias(tuple: Map[String, Any]) {
    val (alias, _) = createAliasFromTuple(tuple)
    aliasMap.remove(alias)
    removeEdge(WordNet.Meta.Aliases, tuple)
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
                destinationNames: List[String], destinations: List[List[Any]]) = atomic { implicit txn =>
    val (addFunction, removeFunction) : (Map[String, Any] => Unit, Map[String, Any] => Unit) = relation match {
      case WordNet.Meta.Relations =>
        (addMetaRelation _, removeMetaRelation _)
      case WordNet.Meta.Arguments =>
        (addMetaArgument _, removeMetaArgument _)
      case WordNet.Meta.Properties =>
        (addMetaProperty _, removeMetaProperty _)
      case WordNet.Meta.PairProperties =>
        (addMetaPairProperty _, removeMetaPairProperty _)
      case WordNet.Meta.Dependencies =>
        (addMetaDependency _, removeMetaDependency _)
      case WordNet.Meta.Aliases =>
        (addMetaAlias _, removeMetaAlias _)
      case _ =>
        (∅[Map[String, Any] => Unit], ∅[Map[String, Any] => Unit])
    }

    relation match {
      case WordNet.SynsetSet =>
        setSynsets(destinations.map(_.head.asInstanceOf[Synset]))
      case WordNet.SenseSet =>
        setSenses(destinations.map(_.head.asInstanceOf[Sense]))
      case WordNet.WordSet =>
        setWords(destinations.map(_.head.asInstanceOf[String]))
      case WordNet.PosSet =>
        setPartOfSpeechSymbols(destinations.map(_.head.asInstanceOf[String]))
      case _ =>
        setLinks(relation, sourceNames, sources, destinationNames, destinations, addFunction, removeFunction)
    }
  }

  def addLink(relation: Relation, tuple: Map[String, Any], addFunction: Map[String, Any] => Unit = ∅[Map[String, Any] => Unit]) = atomic { implicit txn =>
    handleFunctionalForPropertyForAddLink(relation, tuple)
    addEdge(relation, tuple)
    handleSymmetryForAddLink(relation, tuple)
    addFunction(tuple)
  }

  private def addEdge(relation: Relation, tuple: Map[String, Any]) = atomic { implicit txn =>
    val relationSuccessors = store.successors(relation)

    for ((sourceName, sourceValue) <- tuple) {
      if (!(relationSuccessors.contains((sourceName, sourceValue)))) {
        relationSuccessors((sourceName, sourceValue)) = Vector()
      }

      relationSuccessors((sourceName, sourceValue)) = relationSuccessors((sourceName, sourceValue)) :+ tuple
    }

  }

  private def removeEdge(relation: Relation, tuple: Map[String, Any]) = atomic { implicit txn =>
    val relationSuccessors = store.successors(relation)

    for ((sourceName, sourceValue) <- tuple if relationSuccessors.contains((sourceName, sourceValue))) {
      relationSuccessors((sourceName, sourceValue)) = relationSuccessors((sourceName, sourceValue)).filterNot(_ == tuple)
    }

  }

  private def containsLink(relation: Relation, tuple: Map[String, Any]) = atomic { implicit txn =>
    store.successors(relation).get(Relation.Src, tuple(Relation.Src)).some(_.contains(tuple)).none(false)
  }

  private def handleFunctionalForPropertyForAddLink(relation: Relation, tuple: Map[String, Any]) = atomic { implicit txn =>
    val relationSuccessors = store.successors(relation)

    for (param <- ~functionalFor.get(relation) if (tuple.contains(param))) {
      val node = tuple(param)

      relationSuccessors.get((param, node)).map( successors =>
        if (!successors.isEmpty) {
          throw new WQueryUpdateBreaksRelationPropertyException(Relation.Functional, relation, param)
        }
      )
    }
  }

  private def handleSymmetryForAddLink(relation: Relation, tuple: Map[String, Any]) {
    symmetry.get(relation).map { symmetries =>
      for (((src, dst), sym) <- symmetries) {
        sym match {
          case Symmetric =>
            if (symmetryActions((relation, src, dst)) == Relation.Restore)
              addEdge(relation, Map((src, tuple(dst)),(dst, tuple(src))))
            else if (symmetryActions((relation, src, dst)) == Relation.Preserve)
              throw new WQueryUpdateBreaksRelationPropertyException("symmetric", relation)
          case Antisymmetric =>
            if (transitives.get(relation).some(_.contains((src, dst))).none(false)) {
              if (reaches(relation, tuple(dst), tuple(src), Nil))
                throw new WQueryUpdateBreaksRelationPropertyException("transitive antisymmetry", relation)
            } else {
              if (follow(relation, src, tuple(dst), dst).contains(tuple(src))) {
                if (symmetryActions((relation, src, dst)) == Relation.Restore)
                  removeMatchingLinks(relation, Map((src, tuple(dst)), (dst, tuple(src))))
                else if (symmetryActions((relation, src, dst)) == Relation.Preserve)
                  throw new WQueryUpdateBreaksRelationPropertyException("antisymmetry", relation)
              }
            }
          case _ =>
          // do nothing
        }
      }
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

  private def removeNode(domainType: DomainType, value: Any, withDependentNodes: Boolean, withCollectionDependentNodes: Boolean) = atomic { implicit txn =>
    removeDependentLinks(domainType, value, withDependentNodes, withCollectionDependentNodes)
    removeLink(WordNet.dataTypesRelations(domainType), Map((Relation.Src, value)))
  }

  private def removeDependentLinks(nodeType: NodeType, obj: Any, withDependentNodes: Boolean, withCollectionDependentNodes: Boolean) {
    for (relation <- relations) {
      for (trans <- transitives.get(relation); (src, dst) <- trans) {
        if (transitivesActions((relation, src, dst)) == Relation.Restore) {
          for (source <- follow(relation, dst, obj, src);
               destination <- follow(relation, src, obj, dst)) {
            addEdge(relation, Map((src, source), (dst, destination)))
          }
        } else if (transitivesActions((relation, src, dst)) == Relation.Preserve &&
          !follow(relation, dst, obj, src).isEmpty &&
          !follow(relation, src, obj, dst).isEmpty) {
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

  def removeLink(relation: Relation, tuple: Map[String, Any], withDependentNodes: Boolean = true, withCollectionDependentNodes: Boolean = true) = atomic { implicit txn =>
    removeLinkByNode(relation, tuple, None, ∅[Map[String, Any] => Unit],
      withDependentNodes = withDependentNodes, withCollectionDependentNodes = withCollectionDependentNodes)
  }

  def removeLinkByNode(relation: Relation, tuple: Map[String, Any], node: Option[Any], removeFunction: Map[String, Any] => Unit,
                       symmetricEdge: Boolean = false, withDependentNodes: Boolean = true, withCollectionDependentNodes: Boolean = true): Unit = atomic { implicit txn =>
    val relationSuccessors = store.successors(relation)

    for ((sourceName, sourceValue) <- tuple if (relationSuccessors.contains(sourceName, sourceValue))) {
      val updatedSuccessors = relationSuccessors((sourceName, sourceValue)).filterNot(_ == tuple)

      handleRequiredByPropertyForRemoveLink(relation, sourceName, sourceValue, node, updatedSuccessors)
      relationSuccessors((sourceName, sourceValue)) = updatedSuccessors
    }

    if (withDependentNodes) {
      for (dependentArg <- ~dependent.get(relation) if DataType.domain.contains(relation.demandArgument(dependentArg).nodeType))
        removeNode(relation.demandArgument(dependentArg).nodeType.asInstanceOf[DomainType], tuple(dependentArg), true, true)
    }

    if (withCollectionDependentNodes) {
      for (collectionDependentArg <- ~collectionDependent.get(relation) if DataType.domain.contains(relation.demandArgument(collectionDependentArg).nodeType)) {
        val collectionDependentNode = tuple(collectionDependentArg)

        if (relationSuccessors(collectionDependentArg, collectionDependentNode).isEmpty)
          removeNode(relation.demandArgument(collectionDependentArg).nodeType.asInstanceOf[DomainType], collectionDependentNode, true, true)
      }
    }

    handleSymmetryForRemoveLink(relation, tuple, node, symmetricEdge)
    removeFunction(tuple)
  }

  private def handleRequiredByPropertyForRemoveLink(relation: Relation, argumentName: String, argumentValue: Any,
                                                    node: Option[Any], relationSuccessors: Seq[Map[String, Any]]) {
    if (requiredBys.get(relation).some(_.contains(argumentName)).none(false) && node != Some(argumentValue)) {
      if (relationSuccessors.isEmpty)
        throw new WQueryUpdateBreaksRelationPropertyException(Relation.RequiredBy, relation, argumentName)
    }
  }

  private def handleSymmetryForRemoveLink(relation: Relation, tuple: Map[String, Any], node: Option[Any], symmetricEdge: Boolean) {
    if (!symmetricEdge) {
      for (syms <- symmetry.get(relation); ((src, dst), sym) <- syms if sym == Symmetric) {
        removeLinkByNode(relation,
          Map((src, tuple(dst)),(dst, tuple(src))), node, ∅[Map[String, Any] => Unit], symmetricEdge = true)
      }
    }
  }

  def removeMatchingLinks(relation: Relation, tuple: Map[String, Any]) = atomic { implicit txn =>
    removeMatchingLinksByNode(relation, tuple, None, true, true)
  }

  def removeMatchingLinksByNode(relation: Relation, tuple: Map[String, Any], node: Option[Any],
                                withDependentNodes: Boolean, withCollectionDependentNodes: Boolean) = atomic { implicit txn =>
    val relationSuccessors = store.successors(relation)

    for ((sourceName, sourceValue) <- tuple if (relationSuccessors.contains(sourceName, sourceValue))) {
      val matchingSuccessors = relationSuccessors((sourceName, sourceValue)).filter(succ => tuple.forall(elem => succ.exists(_ == elem)))

      matchingSuccessors.foreach(removeLinkByNode(relation, _, node, ∅[Map[String, Any] => Unit], false, withDependentNodes, withCollectionDependentNodes))
    }
  }

  def setLinks(relation: Relation, sourceNames: List[String], sources: List[List[Any]],
               destinationNames: List[String], destinations: List[List[Any]],
               addFunction: Map[String, Any] => Unit, removeFunction: Map[String, Any] => Unit) = atomic { implicit txn =>
    if (sourceNames.isEmpty)
      replaceLinks(relation, destinationNames, destinations, addFunction, removeFunction)
    else
      replaceLinksBySource(relation, sources, sourceNames, destinationNames, destinations, addFunction, removeFunction)
  }

  def replaceLinks(relation: Relation, destinationNames: List[String], destinations: List[List[Any]],
                   addFunction: Map[String, Any] => Unit, removeFunction: Map[String, Any] => Unit) = atomic { implicit txn =>
    store.successors(relation).clear()

    for (destination <- destinations)
      addLink(relation, destinationNames.zip(destination).toMap)
  }

  def replaceLinksBySource(relation: Relation, sources: List[List[Any]], sourceNames: List[String],
                           destinationNames: List[String], destinations: List[List[Any]],
                           addFunction: Map[String, Any] => Unit, removeFunction: Map[String, Any] => Unit) = atomic { implicit txn =>
    val relationSuccessors = store.successors(relation)

    for (source <- sources; i <- 0 until sourceNames.size) {
      val sourceName = sourceNames(i)
      val sourceValue = source(i)
      val sourceSuccessors = relationSuccessors.getOrElse((sourceName, sourceValue), Vector())
      val (matchingSuccessors, _) = sourceSuccessors.partition({
        successor =>
          sourceNames.zip(source).forall {
            case (name, value) => successor(name) == value
          }
      })

      matchingSuccessors.foreach(tuple => removeLinkByNode(relation, tuple, Some(sourceValue), removeFunction))

      for (destination <- destinations) {
        val tuple = (sourceNames.zip(source) ++ destinationNames.zip(destination)).toMap
        addLink(relation, tuple, addFunction)
      }
    }
  }

  private def demandSynset(sense: Sense) = {
    follow(WordNet.SenseToSynset, Relation.Src, sense, Relation.Dst).head.asInstanceOf[Synset]
  }

  def merge(synsetsList: List[Synset], sensesList: List[Sense]) = atomic { implicit txn =>
    if (!synsetsList.isEmpty || !sensesList.isEmpty) {
      val synsets = synsetsList.distinct
      val senses = sensesList.distinct
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

  private def copyLinks(relation: Relation, requiredType: DataType, source: Any, destination: Any) = atomic { implicit txn =>
    val relationSuccessors = store.successors(relation)

    for (argument <- relation.arguments if argument.nodeType == requiredType) {
      for (succs <- relationSuccessors.get((argument.name, source)); successor <- succs) {
        val tuple = ((argument.name, destination)::((successor - argument.name).toList)).toMap

        if (!containsLink(relation, tuple))
          addLink(relation, tuple)
      }
    }
  }
}
