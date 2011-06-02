package org.wquery.model
import org.wquery.WQueryModelException

class WordNet(store: WordNetStore) {
  for (relation <- WordNet.relations)
    if (!store.relations.contains(relation))
      store.add(relation)

  def generateAllTuples(relation: Relation, args: List[String]) = store.generate(relation, args.map(x => (x, List[Any]())), args)

  def synsets: DataSet = generateAllTuples(WordNet.SynsetSet, List(Relation.Source))
  
  def senses: DataSet = generateAllTuples(WordNet.SenseSet, List(Relation.Source))
  
  def words: DataSet = generateAllTuples(WordNet.WordSet, List(Relation.Source))

  def followRelation(dataSet: DataSet, pos: Int, relation: Relation, source: String, dests: List[String]) = {
    store.extend(dataSet, relation, pos, source, dests)
  }

  def followAny(dataSet: DataSet, pos: Int) = {
    val buffer = new DataSetBuffer

    for (relation <- store.relations) {
      for (source <- relation.argumentNames)
        for (dest <- relation.argumentNames)
          if (source != dest)
            buffer.append(store.extend(dataSet, relation, pos, source, List(dest)))
    }

    buffer.toDataSet
  }

  def getSynsetsBySenses(senses: List[Sense]) = {
    store.generate(WordNet.SenseToSynset,  List((Relation.Source, senses)), List(Relation.Destination))
  }

  def getSynsetsByWordForms(wordForms: List[String]) = {
    store.generate(WordNet.WordFormToSynsets,  List((Relation.Source, wordForms)), List(Relation.Destination))
  }

  def getSenseByWordFormAndSenseNumberAndPos(word: String, num: Int, pos: String) = {
    store.generate(WordNet.SenseToWordFormSenseNumberAndPos,
      List((Relation.Destination, List(word)), ("num", List(num)), ("pos", List(pos))), List(Relation.Source))
  }
  
  def getSensesByWordFormAndSenseNumber(word: String, num: Int) = {
    store.generate(WordNet.SenseToWordFormSenseNumberAndPos,
      List((Relation.Destination, List(word)), ("num", List(num))), List(Relation.Source))
  }

  def getWordForm(word: String) = store.generate(WordNet.WordSet, List((Relation.Source, List(word))), List(Relation.Source))
  
  def getRelation(name: String, sourceType: DataType, sourceName: String) = store.relations.find(r => r.name == name && r.arguments.get(sourceName) == Some(sourceType))

  def demandRelation(name: String, sourceType: DataType, sourceName: String) = {
    getRelation(name, sourceType, sourceName).getOrElse(throw new WQueryModelException("Relation '" + name + "' with source type " + sourceType + " not found"))
  }

  def containsRelation(name: String, sourceType: DataType, sourceName: String) = getRelation(name, sourceType, sourceName) != None

  def findRelationsByNameAndSource(name: String, sourceName: String) = {
    List(getRelation(name, SynsetType, sourceName), getRelation(name, SenseType, sourceName), getRelation(name, StringType, sourceName),
      getRelation(name, IntegerType, sourceName), getRelation(name, FloatType, sourceName), getRelation(name, BooleanType, sourceName))
        .filter(_.map(_ => true).getOrElse(false)).map(_.get)
  }
  
  def findFirstRelationByNameAndSource(name: String, sourceName: String) = {
    findRelationsByNameAndSource(name, sourceName) match {
      case head::_ =>
        Some(head)         
      case Nil =>
        None        
    }        
  }

  def addSynset(synset: Synset) {
    store.add(WordNet.SynsetSet, List((Relation.Source, synset)))
    addSuccessor(synset.id, WordNet.IdToSynset, synset)
    addSuccessor(synset, WordNet.SynsetToId, synset.id)

    for (sense <- synset.senses) {
      if (getWordForm(sense.wordForm).isEmpty)
        store.add(WordNet.WordSet, List((Relation.Source, sense.wordForm)))

      store.add(WordNet.SenseSet, List((Relation.Source, sense)))
      addSuccessor(sense.id, WordNet.IdToSense, sense)
      addSuccessor(sense, WordNet.SenseToId, sense.id)
      addSuccessor(sense, WordNet.SenseToWordForm, sense.wordForm)
      addSuccessor(sense, WordNet.SenseToSenseNumber, sense.senseNumber)
      addSuccessor(sense, WordNet.SenseToPos, sense.pos)
      addSuccessor(synset, WordNet.SynsetToSenses, sense)
      addSuccessor(synset, WordNet.SynsetToWordForms, sense.wordForm)
      addSuccessor(sense.wordForm, WordNet.WordFormToSenses, sense)
      addSuccessor(sense,WordNet. SenseToSynset, synset)
      addSuccessor(sense.wordForm, WordNet.WordFormToSynsets, synset)
      addTuple(WordNet.SenseToWordFormSenseNumberAndPos, List((Relation.Source, sense), (Relation.Destination, sense.wordForm), ("num", sense.senseNumber), ("pos", sense.pos)))
    }
  }

  def addTuple(relation: Relation, tuple: List[(String, Any)]) = store.add(relation, tuple)

  def addSuccessor(pred: Any, relation: Relation, succ: Any) = addTuple(relation, List((Relation.Source, pred), (Relation.Destination, succ)))

  def addRelation(relation: Relation) = store.add(relation)
}


object WordNet {
  val SynsetSet = Relation("synset_set", SynsetType)
  val SenseSet = Relation("sense_set", SenseType)
  val WordSet = Relation("word_set", StringType)
  val PosSet = Relation("pos_set", StringType)
  val GlossSet = Relation("gloss_set", StringType)
  val SenseNumSet = Relation("sensenum_set", IntegerType)

  val IdToSynset = Relation("id_synset", StringType, SynsetType)
  val SynsetToId = Relation("id", SynsetType, StringType)
  val IdToSense = Relation("id_sense", StringType, SenseType)
  val SenseToId = Relation("id", SenseType, StringType)  
  val SenseToWordFormSenseNumberAndPos = Relation("literal", SenseType, StringType, Map(("num", IntegerType), ("pos", StringType)))
  val SenseToWordForm = Relation("word", SenseType, StringType)
  val SenseToSenseNumber = Relation("sensenum", SenseType, IntegerType)
  val SenseToPos = Relation("pos", SenseType, StringType)
  val SynsetToWordForms = Relation("words", SynsetType, StringType)
  val SynsetToSenses = Relation("senses", SynsetType, SenseType)
  val WordFormToSenses = Relation("senses", StringType, SenseType)
  val SenseToSynset = Relation("synset", SenseType, SynsetType)
  val WordFormToSynsets = Relation("synsets", StringType, SynsetType)
  
  val relations = List(IdToSynset, SynsetToId, IdToSense, SenseToId, SenseToWordForm, SenseToSenseNumber,
    SenseToPos, SynsetToWordForms, SynsetToSenses, WordFormToSenses, SenseToSynset, WordFormToSynsets,
    SenseToWordFormSenseNumberAndPos)
}