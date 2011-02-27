package org.wquery.model
import org.wquery.WQueryModelException
import scala.collection.mutable.{Map, Set}

trait WordNet {    

  def synsets: Set[Synset]
  
  def senses: Set[Sense]
  
  def words: Set[String]
  
  def relations: Map[(String, DataType, String), Relation]
  
  def follow(dataset: DataSet, pos: Int, relation: Relation, source: String, dests: List[String]): DataSet  
  
  def getPaths(relation: Relation, source: String, dests: List[String]): List[List[Any]]
  
  private def getSuccessors(obj: Any, relation: Relation): List[Any] = {
    follow(DataSet.fromValue(obj), 1, relation, Relation.Source, List(Relation.Destination)).paths.map(_.last) // TO BE rewritten after implementing WordNetStore    
  }
  
  def getSynsetsByWordForm(word: String) = getSuccessors(word, WordNet.WordFormToSynsets)  

  def demandSynsetBySense(sense: Sense) = {
    val succs = getSuccessors(sense, WordNet.SenseToSynset)
    if (!succs.isEmpty) succs.head else throw new WQueryModelException("No synset found for sense " + sense)    
  }
  
  def getSenseByWordFormAndSenseNumberAndPos(word: String, num: Int, pos: String) = {
    val succs = getSuccessors(word + ":" + num + ":" + pos, WordNet.WordFormAndSenseNumberAndPosToSense)
    if (succs.isEmpty) None else Some(succs.head)
  }
  
  def getSensesByWordFormAndSenseNumber(word: String, num: Int) = getSuccessors(word + ":" + num, WordNet.WordFormAndSenseNumberToSenses) 
  
  def getWordForm(word: String) = words.find(_ == word)  

  def demandRelation(name: String, sourceType: DataType, sourceName: String) = {
    relations.getOrElse((name, sourceType, sourceName), throw new WQueryModelException("Relation '" + name + "' with source type " + sourceType + " not found"))
  }  
  
  def getRelation(name: String, sourceType: DataType, sourceName: String) = relations.get((name, sourceType, sourceName))
  
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
  
  def containsRelation(name: String, sourceType: DataType, sourceName: String) = relations.contains(name, sourceType, sourceName)     
}


object WordNet {  
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