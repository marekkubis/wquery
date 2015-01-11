package org.wquery.loader

import java.net.URL

import edu.mit.jwi.item._
import edu.mit.jwi.{Dictionary, IDictionary}
import org.wquery.model.impl.InMemoryWordNet
import org.wquery.model.{Synset, _}
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

  val sensesByWord = mutable.Map[(String, Int, String), Sense]()
  val tagCounts = mutable.Map[Sense, Int]()

  private def mapPointerToRelationName(pointer: IPointer) = {
    pointerMap.getOrElse(pointer, spacesRegex.replaceAllIn(
      pointerNameSpecialCharRegex.replaceAllIn(pointer.getName.toLowerCase, " "), "_"))
  }

  private def mapWordToSense(dict: IDictionary, word: IWord) = {
    val lemma = unescapeWord(word.getLemma)
    val senseKey = word.getSenseKey
    val senseEntry = dict.getSenseEntry(senseKey)
    val senseNumber = if (senseEntry != null) {
      senseEntry.getSenseNumber
    } else {
      log.warn("Sense entry not found for sense key '" + senseKey + "'. Setting sense number to 0.")
      0
    }

    val pos = word.getPOS.getTag.toString

    if (sensesByWord.contains((lemma, senseNumber, pos))) {
      sensesByWord((lemma, senseNumber, pos))
    } else {
      val sense = Sense(lemma, senseNumber, pos)
      sensesByWord.put((lemma, senseNumber, pos), sense)
      tagCounts.put(sense, if (senseEntry != null) senseEntry.getTagCount else 0)
      sense
    }
  }

  private def unescapeWord(word: String) = word.replace('_', ' ')

  override def load(path: String) = {
    val url = new URL("file", null, path)
    val dict = new Dictionary(url)
    val wordNet = new InMemoryWordNet
    val synsetsById = mutable.Map[String, Synset]()

    val adjMarkerRelation = wordNet.addRelation(Relation.binary("adj_marker", SenseType, StringType))
    val glossRelation = wordNet.addRelation(Relation.binary("gloss", SynsetType, StringType))
    val lexicalFileRelation = wordNet.addRelation(Relation.binary("lexical_file", SynsetType, StringType))
    val posRelation = wordNet.addRelation(Relation.binary("pos", SynsetType, POSType))
    val senseKeyRelation = wordNet.addRelation(Relation.binary("sense_key", SenseType, StringType))
    val tagCountRelation = wordNet.addRelation(Relation.binary("tag_count", SenseType, IntegerType))
    val verbFrameRelation = wordNet.addRelation(Relation.binary("verb_frame", SenseType, StringType))

    dict.open()

    for (pos <- POS.values()) {
      for (iSynset <- dict.getSynsetIterator(pos)) {
        val synsetId = iSynset.getID.toString
        val senses = iSynset.getWords.map{ word => mapWordToSense(dict, word)}.toList
        val synset = wordNet.addSynset(Some(synsetId), senses, moveSenses = false)

        synsetsById(synsetId) = synset

        wordNet.addSuccessor(synset, glossRelation, iSynset.getGloss)
        wordNet.addSuccessor(synset, lexicalFileRelation, iSynset.getLexicalFile.getName)
        wordNet.addSuccessor(synset, posRelation, iSynset.getPOS.getTag.toString)

        for (sense <- senses) {
          wordNet.addSuccessor(sense, tagCountRelation, tagCounts(sense))
        }
      }
    }

    for (pos <- POS.values()) {
      for (synset <- dict.getSynsetIterator(pos)) {
        val source = synsetsById(synset.getID.toString)
        for ((pointer, destinations) <- synset.getRelatedMap) {
          val relationName = mapPointerToRelationName(pointer)
          val relation = wordNet.schema.getRelation(relationName, Map((Relation.Src, Set[DataType](SynsetType)))) | {
            wordNet.addRelation(Relation.binary(relationName, SynsetType, SynsetType))
          }

          for (destination <- destinations) {
            val target = synsetsById(destination.toString)

            wordNet.addSuccessor(source, relation, target)
          }
        }

        for (word <- synset.getWords) {
          val sourceSense = mapWordToSense(dict, word)

          for ((pointer, destinations) <- word.getRelatedMap) {
            val relationName = mapPointerToRelationName(pointer)
            val relation = wordNet.schema.getRelation(relationName, Map((Relation.Src, Set[DataType](SenseType)))) | {
              wordNet.addRelation(Relation.binary(relationName, SenseType, SenseType))
            }

            for (destination <- destinations) {
              wordNet.addSuccessor(sourceSense, relation, mapWordToSense(dict, dict.getWord(destination)))
            }
          }

          for (verbFrame <- word.getVerbFrames) {
            wordNet.addSuccessor(sourceSense, verbFrameRelation, verbFrame.getTemplate)
          }

          if (word.getAdjectiveMarker != null) {
            wordNet.addSuccessor(sourceSense, adjMarkerRelation, word.getAdjectiveMarker.getSymbol)
          }

          wordNet.addSuccessor(sourceSense, senseKeyRelation, word.getSenseKey.toString)
        }
      }
    }

    dict.close()
    wordNet
  }
}
