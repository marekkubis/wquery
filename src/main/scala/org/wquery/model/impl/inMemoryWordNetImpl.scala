package org.wquery.model.impl
import org.wquery.model.{Sense, Synset, DataType, Relation, WordNet}
import scala.collection.mutable.{Set, Map}

class InMemoryWordNetImpl (val synsets: Set[Synset], val senses: Set[Sense], val words: Set[String], 
                           val relations: Map[(String, DataType), Relation],
                           val successors: Map[(Any, Relation, String), List[Any]]) extends WordNet {
    
  val predecessors = Map[(Any, Relation, String), List[Any]]()  
  
  for (((obj, rel, dest), succs) <- successors) {
    for (succ <- succs) {
      if (!(predecessors contains (succ, rel, dest)))
        predecessors((succ, rel, dest)) = Nil
    
      predecessors((succ, rel, dest)) = obj :: predecessors((succ, rel, dest))
    }
  }

  def getSuccessors(obj: Any, relation: Relation, destination: String): List[Any] = {
    if (successors contains (obj, relation, destination)) {
        successors((obj, relation, destination))
    } else {
      Nil
    }
  }
  
  def getPredecessors(obj: Any, relation: Relation, destination: String): List[Any] = {
    if (predecessors contains (obj, relation, destination)) {
        predecessors((obj, relation, destination))
    } else {
      Nil
    }
  }  
}

class InMemoryWordNetImplBuilder {
  // wordnet content
  val synsets = Set[Synset]()
  val senses = Set[Sense]()
  val words = Set[String]()  
  val relations = Map[(String, DataType), Relation]()
  val successors = Map[(Any, Relation, String), List[Any]]()       
    
  // import built in relations
  for (relation <- WordNet.relations) {    
    addRelation(relation)
  }
  
  def getSynsetById(id: String) = {
    try {
      val succs = successors((id, WordNet.IdToSynset, Relation.Destination))
      if (succs.isEmpty) None else Some(succs.head)
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
    
    addSuccessor(synset.id, WordNet.IdToSynset, Relation.Destination, synset)    
    addSuccessor(synset, WordNet.SynsetToId, Relation.Destination, synset.id) // TODO change into algebraic transformation
    
    for (sense <- synset.senses) {
      senses += sense
      words += sense.wordForm
      addSuccessor(sense.id, WordNet.IdToSense, Relation.Destination, sense)
      addSuccessor(sense, WordNet.SenseToId, Relation.Destination, sense.id) // TODO change into algebraic transformation
      addSuccessor(sense, WordNet.SenseToWordForm, Relation.Destination, sense.wordForm)      
      addSuccessor(sense, WordNet.SenseToSenseNumber, Relation.Destination, sense.senseNumber)
      addSuccessor(sense, WordNet.SenseToPos, Relation.Destination, sense.pos)
      addSuccessor(synset, WordNet.SynsetToSenses, Relation.Destination, sense)
      addSuccessor(synset, WordNet.SynsetToWordForms, Relation.Destination, sense.wordForm)      
      addSuccessor(sense.wordForm, WordNet.WordFormToSenses, Relation.Destination, sense)
      addSuccessor(sense,WordNet. SenseToSynset, Relation.Destination, synset)
      addSuccessor(sense.wordForm, WordNet.WordFormToSynsets, Relation.Destination, synset)
      addSuccessor(sense.wordForm + ":" + sense.senseNumber + ":" + sense.pos, WordNet.WordFormAndSenseNumberAndPosToSense, Relation.Destination, sense)
      addSuccessor(sense.wordForm + ":" + sense.senseNumber, WordNet.WordFormAndSenseNumberToSenses, Relation.Destination, sense)
    }    
  }  
  
  def addSuccessor(obj: Any, relation: Relation, destination: String, successor: Any) { 
    if (!(successors contains (obj, relation, destination))) 
      successors((obj, relation, destination)) = Nil
    
    successors((obj, relation, destination)) = successor :: successors((obj, relation, destination))
  }  
  
  def addRelation(relation: Relation) = relations((relation.name, relation.sourceType)) = relation
}