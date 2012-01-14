package org.wquery.model

import org.wquery.engine.operations.RelationalPattern
import scala.collection.mutable.{Map => MMap}

trait WordNetStore {
  // querying
  def relations: List[Relation]

  def fetch(relation: Relation, from: List[(String, List[Any])], to: List[String]): DataSet

  def fringe(relation: List[(Relation, String)], distinct: Boolean = true): DataSet

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
  val dependent = MMap[Relation, Set[String]]()
  val collectionDependent = MMap[Relation, Set[String]]()
  val functionalFor = MMap[Relation, Set[String]]()
  val requiredBys = MMap[Relation, Set[String]]()
  val transitives = MMap[Relation, Boolean]()
  val symmetry = MMap[Relation, Symmetry]()

  val transitivesActions = MMap[Relation, String]()
  val symmetryActions = MMap[Relation, String]()
  val functionalForActions = MMap[(Relation, String), String]()

  // helper methods
  def addSuccessor(pred: Any, relation: Relation, succ: Any) = addLink(relation, Map((Relation.Source, pred), (Relation.Destination, succ)))

  def removeSuccessor(pred: Any, relation: Relation, succ: Any, withDependentNodes: Boolean = false, withCollectionDependentNodes:Boolean = false) = {
    removeLink(relation, Map((Relation.Source, pred), (Relation.Destination, succ)), withDependentNodes, withCollectionDependentNodes)
  }
}



