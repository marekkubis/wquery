package org.wquery.loader

import java.net.URL

import edu.mit.jwi.Dictionary
import edu.mit.jwi.item.POS
import org.wquery.model._
import org.wquery.model.impl.InMemoryWordNet
import org.wquery.utils.Logging

import scala.collection.JavaConversions._
import scala.collection.mutable
import scalaz.Scalaz._

class PWNLoader extends WordNetLoader with Logging {
  override def load(path: String) = {
    val url = new URL("file", null, path)
    val dict = new Dictionary(url)
    val wordNet = new InMemoryWordNet
    val synsetsById = mutable.Map[String, Synset]()

    dict.open()

    for (pos <- POS.values()) {
      for(synset <- dict.getSynsetIterator(pos)) {
        val synsetId = synset.getID.toString
        val senses = synset.getWords.map(word => new Sense(word.getLemma, word.getLexicalID, pos.getTag.toString)).toList

        synsetsById(synsetId) = wordNet.addSynset(Some(synsetId), senses, moveSenses = false)
      }
    }

    for (pos <- POS.values()) {
      for(synset <- dict.getSynsetIterator(pos)) {
        val source = synsetsById(synset.getID.toString)
        for ((pointer, destinations) <- synset.getRelatedMap) {
          val relation = wordNet.schema.getRelation(pointer.getName, Map((Relation.Src, Set[DataType](SynsetType))))|{
            wordNet.addRelation(Relation.binary(pointer.getName, SynsetType, SynsetType))
            wordNet.schema.demandRelation(pointer.getName, Map((Relation.Src, Set[DataType](SynsetType))))
          }

          for (destination <- destinations) {
            val target = synsetsById(destination.toString)

            wordNet.addSuccessor(source, relation, target)
          }
        }
      }
    }

    dict.close()
    wordNet
  }
}
