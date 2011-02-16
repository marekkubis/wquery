package org.wquery.model
import org.wquery.WQueryModelException
import scala.collection.mutable.{Map, Set}

trait WordNet {    

  def synsets: Set[Synset]
  
  def senses: Set[Sense]
  
  def words: Set[String]
  
  def relations: Map[(String, DataType, String), Relation]
  
  def follow(content: List[List[Any]], pos: Int, relation: Relation, source: String, dests: List[String]): List[List[Any]]  
  
  private def getSuccessors(obj: Any, relation: Relation): List[Any] = {
    follow(List(List(obj)), 1, relation, Relation.Source, List(Relation.Destination)).map(x => x.last) // TO BE rewritten after implementing WordNetStore
  }
  
  def getSynsetsByWordForm(word: String) = getSuccessors(word, WordNet.WordFormToSynsets)  

  def getSynsetBySense(sense: Sense) = {
    val succs = getSuccessors(sense, WordNet.SenseToSynset)
    if (!succs.isEmpty) succs.head else throw new WQueryModelException("No synset found for sense " + sense)    
  }
  
  def getSenseByWordFormAndSenseNumberAndPos(word: String, num: Int, pos: String) = {
    try {
      val succs = getSuccessors(word + ":" + num + ":" + pos, WordNet.WordFormAndSenseNumberAndPosToSense)
      if (succs.isEmpty) None else Some(succs.head)
    } catch {
      case  _: java.util.NoSuchElementException => None
    }
  }
  
  def getSensesByWordFormAndSenseNumber(word: String, num: Int) = getSuccessors(word + ":" + num, WordNet.WordFormAndSenseNumberToSenses) 
  
  def getWordForm(word: String) = words.find(_ == word)  

  def demandRelation(name: String, sourceType: DataType, sourceName: String) = {
    relations.getOrElse((name, sourceType, sourceName), throw new WQueryModelException("Relation '" + name + "' with source type " + sourceType + " not found"))
  }  
  
  def getRelation(name: String, sourceType: DataType, sourceName: String) = relations.get((name, sourceType, sourceName)) 
  
  def containsRelation(name: String, sourceType: DataType, sourceName: String) = relations.contains(name, sourceType, sourceName)   
  
}


object WordNet {  
  // default relations
  val IdToSynset = Relation("id_synset", StringType, SynsetType)
  val SynsetToId = Relation("id", SynsetType, StringType)
  val IdToSense = Relation("id_sense", StringType, SenseType)
  val SenseToId = Relation("id", SenseType, StringType)  
  val WordFormAndSenseNumberAndPosToSense = Relation("word_num_pos_sense", StringType, SenseType)
  val WordFormAndSenseNumberToSenses = Relation("word_num_sense", StringType, SenseType)
  val SenseToWordFormSenseNumberAndPos = Relation("literal", SenseType, StringType, scala.collection.immutable.Map(("num", IntegerType), ("pos", StringType)))
  val SenseToWordForm = Relation("word", SenseType, StringType)
  val SenseToSenseNumber = Relation("sensenum", SenseType, IntegerType)
  val SenseToPos = Relation("pos", SenseType, StringType)
  val SynsetToWordForms = Relation("words", SynsetType, StringType)
  val SynsetToSenses = Relation("senses", SynsetType, SenseType)
  val WordFormToSenses = Relation("senses", StringType, SenseType)
  val SenseToSynset = Relation("synset", SenseType, SynsetType)
  val WordFormToSynsets = Relation("synsets", StringType, SynsetType)
  
  val relations = List(IdToSynset, SynsetToId, IdToSense, SenseToId, WordFormAndSenseNumberAndPosToSense,
    WordFormAndSenseNumberToSenses, SenseToWordForm, SenseToSenseNumber, SenseToPos, SynsetToWordForms,
    SynsetToSenses, WordFormToSenses, SenseToSynset, WordFormToSynsets, SenseToWordFormSenseNumberAndPos)
}