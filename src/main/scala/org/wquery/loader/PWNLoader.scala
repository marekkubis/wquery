package org.wquery.loader

import java.net.URL

import edu.mit.jwi.Dictionary
import edu.mit.jwi.item.{IPointer, IWord, POS, Pointer}
import org.wquery.model._
import org.wquery.model.impl.InMemoryWordNet
import org.wquery.utils.Logging

import scala.collection.JavaConversions._
import scala.collection.mutable
import scalaz.Scalaz._

class PWNLoader extends WordNetLoader with Logging {
  private val pointerMap = Map[IPointer, String](
    Pointer.ALSO_SEE -> "also_see",
    Pointer.ANTONYM -> "antonym",
    Pointer.ATTRIBUTE -> "attribute",
    Pointer.CAUSE -> "cause",
    Pointer.DERIVATIONALLY_RELATED -> "derived",
    Pointer.DERIVED_FROM_ADJ -> "derived_adj",
    Pointer.DOMAIN -> "domain",
    Pointer.ENTAILMENT -> "entailment",
    Pointer.HYPERNYM -> "hypernym",
    Pointer.HYPERNYM_INSTANCE -> "instance_hypernym",
    Pointer.HYPONYM -> "hyponym",
    Pointer.HYPONYM_INSTANCE -> "instance_hyponym",
    Pointer.HOLONYM_MEMBER -> "member_holonym",
    Pointer.HOLONYM_SUBSTANCE -> "substance_holonym",
    Pointer.HOLONYM_PART -> "part_holonym",
    Pointer.MEMBER -> "domain_member",
    Pointer.MERONYM_MEMBER -> "member_meronym",
    Pointer.MERONYM_SUBSTANCE -> "substance_meronym",
    Pointer.MERONYM_PART -> "part_meronym",
    Pointer.PARTICIPLE -> "participle",
    Pointer.PERTAINYM -> "pertainym",
    Pointer.REGION -> "region",
    Pointer.REGION_MEMBER -> "region_member",
    Pointer.SIMILAR_TO -> "similar_to",
    Pointer.TOPIC -> "topic",
    Pointer.TOPIC_MEMBER -> "topic_member",
    Pointer.USAGE -> "usage",
    Pointer.USAGE_MEMBER -> "usage_member",
    Pointer.VERB_GROUP -> "verb_group"
  )

  private val pointerNameSpecialCharRegex = "[^a-z]".r

  private val spacesRegex = " +".r

  private def mapPointerToRelationName(pointer: IPointer) = {
    pointerMap.getOrElse(pointer, spacesRegex.replaceAllIn(
      pointerNameSpecialCharRegex.replaceAllIn(pointer.getName.toLowerCase, " "), "_"))
  }

  private def unescapeWord(word: String) = word.replace('_', ' ')

  private def mapWordToSense(word: IWord) = {
    Sense(unescapeWord(word.getLemma), word.getLexicalID + 1, word.getPOS.getTag.toString)
  }

  override def load(path: String) = {
    val url = new URL("file", null, path)
    val dict = new Dictionary(url)
    val wordNet = new InMemoryWordNet
    val synsetsById = mutable.Map[String, Synset]()

    dict.open()

    for (pos <- POS.values()) {
      for (synset <- dict.getSynsetIterator(pos)) {
        val synsetId = synset.getID.toString
        val senses = synset.getWords.map(word => mapWordToSense(word)).toList

        synsetsById(synsetId) = wordNet.addSynset(Some(synsetId), senses, moveSenses = false)
      }
    }

    for (pos <- POS.values()) {
      for (synset <- dict.getSynsetIterator(pos)) {
        val source = synsetsById(synset.getID.toString)
        for ((pointer, destinations) <- synset.getRelatedMap) {
          val relationName = mapPointerToRelationName(pointer)
          val relation = wordNet.schema.getRelation(relationName, Map((Relation.Src, Set[DataType](SynsetType)))) | {
            wordNet.addRelation(Relation.binary(relationName, SynsetType, SynsetType))
            wordNet.schema.demandRelation(relationName, Map((Relation.Src, Set[DataType](SynsetType))))
          }

          for (destination <- destinations) {
            val target = synsetsById(destination.toString)

            wordNet.addSuccessor(source, relation, target)
          }
        }

        for (word <- synset.getWords) {
          for ((pointer, destinations) <- word.getRelatedMap) {
            val sourceSense = mapWordToSense(word)
            val relationName = mapPointerToRelationName(pointer)
            val relation = wordNet.schema.getRelation(relationName, Map((Relation.Src, Set[DataType](SenseType)))) | {
              wordNet.addRelation(Relation.binary(relationName, SenseType, SenseType))
              wordNet.schema.demandRelation(relationName, Map((Relation.Src, Set[DataType](SenseType))))
            }

            for (destination <- destinations) {
              wordNet.addSuccessor(sourceSense, relation, mapWordToSense(dict.getWord(destination)))
            }
          }
        }
      }
    }

    dict.close()
    wordNet
  }
}
