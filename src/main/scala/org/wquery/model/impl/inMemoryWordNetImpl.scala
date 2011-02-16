package org.wquery.model.impl

import org.wquery.model.Arc
import org.wquery.model.{Sense, Synset, DataType, Relation, WordNet}
import scala.collection.mutable.{Set, Map, ListBuffer}

class InMemoryWordNetImpl (val synsets: Set[Synset], val senses: Set[Sense], val words: Set[String], 
                           val relations: Map[(String, DataType), Relation],
                           val successors: Map[(Any, Relation, String), List[Map[String, Any]]]) extends WordNet { 
  
  def follow(content: List[List[Any]], pos: Int, relation: Relation, source: String, dests: List[String]) = {
    relation.reqArgument(source)
    dests.foreach(relation.reqArgument)
      
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
  val relations = Map[(String, DataType), Relation]()
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
  
  def getRelation(name: String, sourceType: DataType) = { 
    if (relations contains (name, sourceType)) Some(relations((name, sourceType))) else None
  }  
  
  def build = new InMemoryWordNetImpl(synsets, senses, words, relations, successors)  
  
  def addSynset(synset: Synset) {
    synsets += synset
    
    addSuccessor(synset.id, WordNet.IdToSynset, Relation.Source, synset)    
    addSuccessor(synset, WordNet.SynsetToId, Relation.Source, synset.id) // TODO change into algebraic transformation
    
    for (sense <- synset.senses) {
      senses += sense
      words += sense.wordForm
      addSuccessor(sense.id, WordNet.IdToSense, Relation.Source, sense)
      addSuccessor(sense, WordNet.SenseToId, Relation.Source, sense.id) // TODO change into algebraic transformation
      addSuccessor(sense, WordNet.SenseToWordForm, Relation.Source, sense.wordForm)      
      addSuccessor(sense, WordNet.SenseToSenseNumber, Relation.Source, sense.senseNumber)
      addSuccessor(sense, WordNet.SenseToPos, Relation.Source, sense.pos)
      addSuccessor(synset, WordNet.SynsetToSenses, Relation.Source, sense)
      addSuccessor(synset, WordNet.SynsetToWordForms, Relation.Source, sense.wordForm)      
      addSuccessor(sense.wordForm, WordNet.WordFormToSenses, Relation.Source, sense)
      addSuccessor(sense,WordNet. SenseToSynset, Relation.Source, synset)
      addSuccessor(sense.wordForm, WordNet.WordFormToSynsets, Relation.Source, synset)
      addSuccessor(sense.wordForm + ":" + sense.senseNumber + ":" + sense.pos, WordNet.WordFormAndSenseNumberAndPosToSense, Relation.Source, sense)
      addSuccessor(sense.wordForm + ":" + sense.senseNumber, WordNet.WordFormAndSenseNumberToSenses, Relation.Source, sense)
    }    
  }   
  
  def addSuccessor(obj: Any, relation: Relation, source: String, successor: Any) { 
    addLink(obj, relation, source, Relation.Destination, successor)
    addLink(successor, relation, Relation.Destination, source, obj)    
  } 
  
  private def addLink(obj: Any, relation: Relation, source: String, destination: String, successor: Any) {
    if (!(successors contains (obj, relation, source))) { 
      successors((obj, relation, source)) = Nil      
    }
    
    successors((obj, relation, source)) = Map((destination, successor)) :: successors((obj, relation, source))      
  }
  
  def addRelation(relation: Relation) = relations((relation.name, relation.sourceType)) = relation
}