package org.wquery.model

import scala.collection.mutable.{Map => MMap}

trait WordNet {
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


object WordNet {
  object Meta {
    // src = name
    val Relations = Relation.unary("relations", StringType)
    // src = relation, dst = argument
    val Arguments = Relation("arguments",
      Set(Argument(Relation.Source, StringType), Argument(Relation.Destination, StringType), Argument("type", StringType), Argument("position", IntegerType)))
    // src = relation, dst = argument
    val Properties = Relation("properties",
      Set(Argument(Relation.Source, StringType), Argument(Relation.Destination, StringType), Argument("property", StringType)))
    // src = relation, dst = source
    val PairProperties = Relation("pair_properties",
      Set(Argument(Relation.Source, StringType),
        Argument(Relation.Destination, StringType), Argument("destination", StringType),
        Argument("property", StringType), Argument("action", StringType)))
    // src = relation, dst = source
    val Aliases = Relation("aliases",
      Set(Argument(Relation.Source, StringType), Argument(Relation.Destination, StringType), Argument("destination", StringType), Argument("name", StringType)))

    val relations = List(Relations, Arguments, Properties, PairProperties, Aliases)
  }

  val SynsetSet = Relation.unary("synsets", SynsetType)
  val SenseSet = Relation.unary("senses", SenseType)
  val WordSet = Relation.unary("words", StringType)
  val PosSet = Relation.unary("possyms", POSType)
  val dataTypesRelations = Map[DataType, Relation](SynsetType -> SynsetSet, SenseType -> SenseSet, StringType -> WordSet, POSType -> PosSet)
  val domainRelations = dataTypesRelations.values.toList

  val IdToSynset = Relation.binary("id_synset", StringType, SynsetType)
  val SynsetToId = Relation.binary("id", SynsetType, StringType)
  val IdToSense = Relation.binary("id_sense", StringType, SenseType)
  val SenseToId = Relation.binary("id", SenseType, StringType)
  val SenseToWordFormSenseNumberAndPos = Relation("literal",
    Set(Argument(Relation.Source, SenseType), Argument(Relation.Destination, StringType), Argument("num", IntegerType), Argument("pos", POSType)))
  val SenseToSenseNumber = Relation.binary("sensenum", SenseType, IntegerType)
  val SenseToPos = Relation.binary("pos", SenseType, POSType)
  val SenseToWordForm = Relation.binary("word", SenseType, StringType)
  val WordFormToSenses = Relation.binary("senses", StringType, SenseType)
  val SenseToSynset = Relation.binary("synset", SenseType, SynsetType)
  val SynsetToSenses = Relation.binary("senses", SynsetType, SenseType)
  val SynsetToWordForms = Relation.binary("words", SynsetType, StringType)
  val WordFormToSynsets = Relation.binary("synsets", StringType, SynsetType)

  val relations = List(IdToSynset, SynsetToId, IdToSense, SenseToId, SenseToWordForm, SenseToSenseNumber,
    SenseToPos, SynsetToWordForms, SynsetToSenses, WordFormToSenses, SenseToSynset, WordFormToSynsets,
    SenseToWordFormSenseNumberAndPos, SynsetSet, SenseSet, WordSet, PosSet)

  val dependent = List((SenseToWordFormSenseNumberAndPos, Set(Relation.Source)), (SenseToPos, Set(Relation.Source)),
    (SenseToWordForm, Set(Relation.Source)), (WordFormToSenses, Set(Relation.Destination)),
    (SenseToSynset, Set(Relation.Source)), (SynsetToSenses, Set(Relation.Destination)))

  val collectionDependent = List((SenseToSynset, Set(Relation.Destination)))
}
