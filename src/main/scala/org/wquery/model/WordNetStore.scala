package org.wquery.model

import org.wquery.engine.{Direction, ArcPattern, RelationalPattern}


trait WordNetStore {
  // querying
  def relations: List[Relation]

  def fetch(relation: Relation, from: List[(String, List[Any])], to: List[String]): DataSet

  def extend(extensionSet: ExtensionSet, relation: Relation, from: Int, direction: Direction, through: String, to: List[String]): ExtendedExtensionSet

  def extend(extensionSet: ExtensionSet, from: Int, direction: Direction, through: (String, Option[NodeType]), to: List[(String, Option[NodeType])]): ExtendedExtensionSet

  def getSenses(synset: Synset): List[Sense]

  def getSynset(sense: Sense): Option[Synset]

  // updating nodes
  def addSense(sense: Sense, assignments: List[PropertyAssignment])

  def addSynset(synsetId: Option[String], senses: List[Sense], assignments: List[PropertyAssignment], moveSenses: Boolean = true): Synset

  def addWord(word: String, patterns: List[PropertyAssignment])

  def addPartOfSpeechSymbol(pos: String, patterns: List[PropertyAssignment])

  def removeSense(sense: Sense)

  def removeSynset(synset: Synset)

  def removeWord(word: String)

  def removePartOfSpeechSymbol(pos: String)

  def setSynsets(synsets: List[Synset], assignments: List[PropertyAssignment])

  def setSenses(newSenses: List[Sense], assignments: List[PropertyAssignment])

  def setPartOfSpeechSymbols(newPartOfSpeechSymbols: List[String], assignments: List[PropertyAssignment])

  def setWords(newWords: List[String], assignments: List[PropertyAssignment])

  // updating relations
  def addRelation(relation: Relation)

  def addRelationPattern(relation: Relation, pattern: RelationalPattern)

  def removeRelation(relation: Relation)

  def setRelations(newRelations: List[Relation], assignments: List[PropertyAssignment])

  // updating links
  def addLink(relation: Relation, tuple: Map[String, Any])

  def removeLink(relation: Relation, tuple: Map[String, Any], withDependentNodes: Boolean = true, withCollectionDependentNodes: Boolean = true)

  def removeMatchingLinks(relation: Relation, tuple: Map[String, Any])

  def setLinks(relation: Relation, sourceName: String, sourceValue: Any, tuples: Seq[Map[String, Any]])

  // merge & split
  def merge(synsets: List[Synset], senses: List[Sense], assignments: List[PropertyAssignment])

  def split(synsets: List[Synset], assignments: List[PropertyAssignment])

  // relation properties
  val dependent = scala.collection.mutable.Map[Relation, Set[String]]()
  val collectionDependent = scala.collection.mutable.Map[Relation, Set[String]]()
  val functionalFor = scala.collection.mutable.Map[Relation, Set[String]]()
  val requiredBys = scala.collection.mutable.Map[Relation, Set[String]]()
  val transitives = scala.collection.mutable.Map[Relation, Boolean]()
  val symmetry = scala.collection.mutable.Map[Relation, Symmetry]()

  val transitivesActions = scala.collection.mutable.Map[Relation, String]()
  val symmetryActions = scala.collection.mutable.Map[Relation, String]()
  val functionalForActions = scala.collection.mutable.Map[(Relation, String), String]()

  // helper methods
  def addSuccessor(pred: Any, relation: Relation, succ: Any) = addLink(relation, Map((Relation.Source, pred), (Relation.Destination, succ)))

  def removeSuccessor(pred: Any, relation: Relation, succ: Any, withDependentNodes: Boolean = false, withCollectionDependentNodes:Boolean = false) = {
    removeLink(relation, Map((Relation.Source, pred), (Relation.Destination, succ)), withDependentNodes, withCollectionDependentNodes)
  }

}

case class PropertyAssignment(pattern: ArcPattern, op: String, values: DataSet) {
  def tuplesFor(node: Any) = {
    val destinationNames = pattern.destinations.map(_.name)
    (for (args <- values.paths) yield ((pattern.source.name, node)::(destinationNames.zip(args.slice(args.size - pattern.destinations.size, args.size)))).toMap)
  }
}

