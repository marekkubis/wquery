package org.wquery.model

class WordNet(val store: WordNetStore) {
  for (relation <- WordNet.relations)
    store.addRelation(relation)

  store.dependent ++= WordNet.dependent
  store.collectionDependent ++= WordNet.collectionDependent
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
