package org.wquery.model

import org.wquery.WQueryModelException

import scala.collection.mutable.Set
import scala.collection.mutable.Map
import java.lang.reflect.Method

trait WordNetStore {
  
  def synsets: Set[Synset]
  
  def senses: Set[Sense]
  
  def words: Set[String]

  //def follow(objects: List[Any], link: String): List[Any] // DataSet -> DataSet tak czy n-arg?
  def follow // from object as param to params
  
  // getobjectsbycond
  
  // create objects

  def addLink(relation: Relation, objects: List[Any])
  
  def removeLink(relation: Relation, objects: List[Any])
  
  def flush  
}

trait WordNetAlgebra { // create algebra.scala ????
  def mpath_union
  def mpath_except
  def mpath_intersect
  
  def path_select  
  def path_dot
  def pat_comma
  def path_filter
  def rel_or
  def rel_and
  def rel_inv
  def rel_tc
}


// plus default impl of algebra using store


trait WordNet {    

  def synsets: Set[Synset]
  
  def senses: Set[Sense]
  
  def words: Set[String]
  
  def relations: Map[(String, DataType), Relation]
  
  def getSuccessors(obj: Any, relation: Relation, destination: String): List[Any]
  
  def getPredecessors(obj: Any, relation: Relation, destination: String): List[Any]  
  
  def getSynsetsByWordForm(word: String) = getSuccessors(word, WordNet.WordFormToSynsets, Relation.Destination)  

  def getSynsetBySense(sense: Sense) = {
    val succs = getSuccessors(sense, WordNet.SenseToSynset, Relation.Destination)
    if (!succs.isEmpty) succs.head else throw new WQueryModelException("No synset found for sense " + sense)    
  }
  
  def getSenseByWordFormAndSenseNumberAndPos(word: String, num: Int, pos: String) = {
    try {
      val succs = getSuccessors(word + ":" + num + ":" + pos, WordNet.WordFormAndSenseNumberAndPosToSense, Relation.Destination)
      if (succs.isEmpty) None else Some(succs.head)
    } catch {
      case  _: java.util.NoSuchElementException => None
    }
  }
  
  def getSensesByWordFormAndSenseNumber(word: String, num: Int) = getSuccessors(word + ":" + num, WordNet.WordFormAndSenseNumberToSenses, Relation.Destination) 
  
  def getWordForm(word: String) = if (words contains word) Some(word) else None  

  def demandRelation(name: String, sourceType: DataType) = {
    if (relations contains (name, sourceType)) {
      relations((name, sourceType))  
    } else {
      throw new WQueryModelException("Relation '" + name + "' with source type " + sourceType + " not found")
    }
  }  
  
  def getRelation(name: String, sourceType: DataType) = { 
    if (relations contains (name, sourceType)) Some(relations((name, sourceType))) else None
  }
  
  def containsRelation(name: String, sourceType: DataType) = relations contains (name, sourceType)   
  
}


object WordNet {  
  // default relations
  val IdToSynset = Relation("id_synset", StringType, SynsetType)
  val SynsetToId = Relation("id", SynsetType, StringType)
  val IdToSense = Relation("id_sense", StringType, SenseType)
  val SenseToId = Relation("id", SenseType, StringType)  
  val WordFormAndSenseNumberAndPosToSense = Relation("word_num_pos_sense", StringType, SenseType)
  val WordFormAndSenseNumberToSenses = Relation("word_num_sense", StringType, SenseType)
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
    SynsetToSenses, WordFormToSenses, SenseToSynset, WordFormToSynsets)
}