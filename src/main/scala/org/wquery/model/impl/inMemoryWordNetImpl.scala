package org.wquery.model.impl

import org.wquery.model.Arc
import org.wquery.model.{Sense, Synset, DataType, Relation, WordNet}
import scala.collection.mutable.{Set, Map, ListBuffer}

class InMemoryWordNetImpl (val synsets: Set[Synset], val senses: Set[Sense], val words: Set[String], 
                           val relations: Map[(String, DataType, String), Relation],
                           val successors: Map[(Any, Relation, String), List[Map[String, Any]]]) extends WordNet { 
  
  def follow(content: List[List[Any]], pos: Int, relation: Relation, source: String, dests: List[String]) = {
    relation.demandArgument(source)
    dests.foreach(relation.demandArgument)
      
    val buffer = new ListBuffer[List[Any]]                      
    
    for (tuple <- content) {
      if (successors.contains((tuple(tuple.size - pos), relation, source))) {
        for (succs <- successors(tuple(tuple.size - pos), relation, source)) {
         val tupleBuffer = new ListBuffer[Any]
         tupleBuffer.appendAll(tuple)
         dests.foreach { dest => 
           if (succs.contains(dest)) {
             tupleBuffer.append(Arc(relation, source, dest))
             tupleBuffer.append(succs(dest))
           }
         }
         buffer.append(tupleBuffer.toList)
        }
      }
    }

    buffer.toList
  }
}

class InMemoryWordNetImplBuilder {
  // wordnet content
  val synsets = Set[Synset]()
  val senses = Set[Sense]()
  val words = Set[String]()  
  val relations = Map[(String, DataType, String), Relation]()
  val successors = Map[(Any, Relation, String), List[Map[String, Any]]]()       
    
  // import built in relations
  for (relation <- WordNet.relations) {    
    addRelation(relation)
  }
  
  def getSynsetById(id: String) = {
    try {
      val succs = successors((id, WordNet.IdToSynset, Relation.Source))
      if (succs.isEmpty) None else Some(succs.head(Relation.Destination))
    } catch {
      case  _: java.util.NoSuchElementException => None
    }
  }  
  
  def getRelation(name: String, sourceType: DataType, sourceName: String) = relations.get(name, sourceType, sourceName)
  
  def build = new InMemoryWordNetImpl(synsets, senses, words, relations, successors)  
  
  def addSynset(synset: Synset) {
    synsets += synset
    
    addSuccessor(synset.id, WordNet.IdToSynset, synset)    
    addSuccessor(synset, WordNet.SynsetToId, synset.id)
    
    for (sense <- synset.senses) {
      senses += sense
      words += sense.wordForm
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
      addSuccessor(sense.wordForm + ":" + sense.senseNumber + ":" + sense.pos, WordNet.WordFormAndSenseNumberAndPosToSense, sense)
      addSuccessor(sense.wordForm + ":" + sense.senseNumber, WordNet.WordFormAndSenseNumberToSenses, sense)
      addTuple(WordNet.SenseToWordFormSenseNumberAndPos, Map((Relation.Source, sense), (Relation.Destination, sense.wordForm), ("num", sense.senseNumber), ("pos", sense.pos)))      
    }    
  }   
  
  def addTuple(relation: Relation, params: Map[String, Any]) {
    for ((sourceName: String, sourceValue: Any) <- params) {
      if (!(successors.contains(sourceValue, relation, sourceName))) { 
        successors((sourceValue, relation, sourceName)) = Nil      
      }
    
      successors((sourceValue, relation, sourceName)) = params :: successors((sourceValue, relation, sourceName))
    }
  }   
  
  def addSuccessor(pred: Any, relation: Relation, succ: Any) = addTuple(relation, Map((Relation.Source, pred), (Relation.Destination, succ)))
  
  def addRelation(relation: Relation) = {
    for ((paramName, paramType) <- relation.arguments) {      
      relations((relation.name, paramType, paramName)) = relation
    }
  }
}