package org.wquery.model

class WordNet(val store: WordNetStore) {
  val schema = new WordNetSchema(store)

  for (relation <- WordNet.relations)
    store.addRelation(relation)

  store.dependent ++= WordNet.dependent
  store.collectionDependent ++= WordNet.collectionDependent
}


object WordNet {
  val SynsetSet = Relation.unary("synsets", SynsetType)
  val SenseSet = Relation.unary("senses", SenseType)
  val WordSet = Relation.unary("words", StringType)
  val PosSet = Relation.unary("possyms", POSType)

  val IdToSynset = Relation.binary("id_synset", StringType, SynsetType)
  val SynsetToId = Relation.binary("id", SynsetType, StringType)
  val IdToSense = Relation.binary("id_sense", StringType, SenseType)
  val SenseToId = Relation.binary("id", SenseType, StringType)
  val SenseToWordFormSenseNumberAndPos = Relation("literal", Set(Argument(Relation.Source, SenseType), Argument(Relation.Destination, StringType), Argument("num", IntegerType), Argument("pos", POSType)))
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