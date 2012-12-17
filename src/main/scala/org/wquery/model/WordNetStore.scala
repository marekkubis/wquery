package org.wquery.model

import org.wquery.engine.operations.RelationalPattern
import scala.collection.mutable.{Map => MMap}

trait WordNetStore {
  // querying
  def relations: List[Relation]

  def fetch(relation: Relation, from: List[(String, List[Any])], to: List[String]): DataSet

  def fringe(relation: List[(Relation, String)], distinct: Boolean = true): DataSet

  def extend(extensionSet: ExtensionSet, relation: Relation, direction: Direction, through: String, to: List[String]): ExtendedExtensionSet

  def extend(extensionSet: ExtensionSet, direction: Direction, through: (String, Option[NodeType]), to: List[(String, Option[NodeType])]): ExtendedExtensionSet

  def getSenses(synset: Synset): List[Sense]

  def getSynset(sense: Sense): Option[Synset]

  // schema
  def schema: WordNetSchema

  // stats
  def stats: WordNetStats

  // updating nodes
  def addSynset(synsetId: Option[String], senses: List[Sense], moveSenses: Boolean = true): Synset

  def removeSense(sense: Sense)

  def removeSynset(synset: Synset)

  def removeWord(word: String)

  def removePartOfSpeechSymbol(pos: String)

  def setSynsets(synsets: List[Synset])

  def setSenses(newSenses: List[Sense])

  def setPartOfSpeechSymbols(newPartOfSpeechSymbols: List[String])

  def setWords(newWords: List[String])

  // updating relations
  def addRelation(relation: Relation)

  def addRelationPattern(relation: Relation, pattern: RelationalPattern)

  def removeRelation(relation: Relation)

  def setRelations(newRelations: List[Relation])

  // updating tuples
  def addTuple(relation: Relation, tuple: Map[String, Any])

  def removeTuple(relation: Relation, tuple: Map[String, Any], withDependentNodes: Boolean = true, withCollectionDependentNodes: Boolean = true)

  def setTuples(relation: Relation, sourceNames: List[String], sourceValues: List[List[Any]],
                destinationNames: List[String], destinationValues: List[List[Any]])

  // merge & split
  def merge(synsets: List[Synset], senses: List[Sense])

  def split(synsets: List[Synset])

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
  def addSuccessor(predecessor: Any, relation: Relation, successor: Any) {
    addTuple(relation, Map((Relation.Source, predecessor), (Relation.Destination, successor)))
  }

  def removeSuccessor(predecessor: Any, relation: Relation, successor: Any, withDependentNodes: Boolean = false, withCollectionDependentNodes:Boolean = false) {
    removeTuple(relation, Map((Relation.Source, predecessor), (Relation.Destination, successor)), withDependentNodes, withCollectionDependentNodes)
  }
}



