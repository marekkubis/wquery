package org.wquery.model

import org.wquery.{WQueryModelException, WQueryProperties}

import scala.collection.mutable.{Map => MMap}
import scalaz.Scalaz._

trait WordNet {
  class Schema {
    def relations = WordNet.this.relations

    def getRelation(name: String, arguments: Map[String, Set[DataType]], includingMeta: Boolean = false) = {
      val relations = WordNet.this.relations ++ (includingMeta ?? WordNet.Meta.relations) ++ WordNet.this.aliases
      relations.find(r => relationMatchesNameAndArguments(r, name, arguments))
    }

    private def relationMatchesNameAndArguments(relation: Relation, name: String, arguments: Map[String, Set[DataType]]) = {
      val extendedArguments = arguments.map{ case (name, types) => types.contains(StringType) ? (name, types + POSType) | (name, types)}
      relation.name == name &&
        extendedArguments.forall{ case (name, types) => relation.getArgument(name).some(arg => types.contains(arg.nodeType)).none(false) }
    }

    def demandRelation(name: String, arguments: Map[String, Set[DataType]], includingMeta: Boolean = false) = {
      getRelation(name, arguments, includingMeta)|(throw new WQueryModelException("Relation '" + name + "' not found"))
    }

    def containsRelation(name: String, arguments: Map[String, Set[DataType]], includingMeta: Boolean = false) = {
      getRelation(name, arguments, includingMeta).isDefined
    }
  }

  val version = WQueryProperties.version // for the purpose of serialization

  // querying
  def relations: List[Relation]

  def aliases: List[Relation]

  def fetch(relation: Relation, from: List[(String, List[Any])], to: List[String], withArcs: Boolean = false): DataSet

  def extend(extensionSet: ExtensionSet, relation: Relation, inverted: Boolean): ExtendedExtensionSet

  def extend(extensionSet: ExtensionSet, through: Option[NodeType], to: Option[NodeType], inverted: Boolean): ExtendedExtensionSet

  def getSenses(synset: Synset): List[Sense]

  def getSenses(word: String): List[Sense]

  def getSynset(sense: Sense): Option[Synset]

  // schema
  val schema: WordNet#Schema = new Schema

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

  def merge(synsets: List[Synset], senses: List[Sense])

  // helper methods
  def addSuccessor(predecessor: Any, relation: Relation, successor: Any) {
    addTuple(relation, Map((Relation.Src, predecessor), (Relation.Dst, successor)))
  }

  def removeSuccessor(predecessor: Any, relation: Relation, successor: Any, withDependentNodes: Boolean = false, withCollectionDependentNodes:Boolean = false) {
    removeTuple(relation, Map((Relation.Src, predecessor), (Relation.Dst, successor)), withDependentNodes, withCollectionDependentNodes)
  }
}

// scalastyle:off multiple.string.literals
object WordNet {
  object Meta {
    val Relations = new Relation("relations", List(Argument("name", StringType))) {
      val Name = "name"
    }

    val Arguments = new Relation("arguments", List(Argument("relation", StringType), Argument("argument", StringType),
      Argument("type", StringType), Argument("position", IntegerType))) {
      val Relation = "relation"
      val Argument = "argument"
      val Type = "type"
      val Position = "position"
    }

    val Properties = new Relation("properties", List(Argument("relation", StringType), Argument("argument", StringType),
      Argument("property", StringType))) {
      val Relation = "relation"
      val Argument = "argument"
      val Property = "property"
      val PropertyValueRequired = "required"
      val PropertyValueFunctional = "functional"
    }

    val PairProperties = new Relation("pair_properties", List(Argument("relation", StringType), Argument("source", StringType),
      Argument("destination", StringType), Argument("property", StringType), Argument("action", StringType))) {
      val Relation = "relation"
      val Source = "source"
      val Destination = "destination"
      val Property = "property"
      val Action = "action"
      val PropertyValueSymmetric = "symmetric"
      val PropertyValueAntisymmetric = "antisymmetric"
      val PropertyValueNonSymmetric = "nonsymmetric"
      val PropertyValueTransitive = "transitive"
    }

    val Dependencies = new Relation("dependencies", List(Argument("relation", StringType), Argument("argument", StringType),
      Argument("type", StringType))) {
      val Relation = "relation"
      val Argument = "argument"
      val Type = "type"
      val TypeValueMember = "member"
      val TypeValueSet = "set"
    }

    val Aliases = new Relation("aliases", List(Argument("relation", StringType), Argument("source", StringType),
      Argument("destination", StringType), Argument("name", StringType))) {
      val Relation = "relation"
      val Source = "source"
      val Destination = "destination"
      val Name = "name"
    }

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
    List(Argument(Relation.Src, SenseType), Argument(Relation.Dst, StringType), Argument("num", IntegerType), Argument("pos", POSType)))
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

  val dependent = List((SenseToWordFormSenseNumberAndPos, Set(Relation.Src)), (SenseToPos, Set(Relation.Src)),
    (SenseToWordForm, Set(Relation.Src)), (WordFormToSenses, Set(Relation.Dst)),
    (SenseToSynset, Set(Relation.Src)), (SynsetToSenses, Set(Relation.Dst)))

  val collectionDependent = List((SenseToSynset, Set(Relation.Dst)))
}
