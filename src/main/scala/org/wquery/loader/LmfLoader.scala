package org.wquery.loader

import org.wquery.utils.Logging
import org.wquery.model.impl.InMemoryWordNetStore
import javax.xml.parsers.SAXParserFactory
import java.io.{File, FileReader, BufferedReader}
import org.xml.sax.helpers.DefaultHandler
import org.xml.sax.{Attributes, Locator}
import collection.mutable.{ListBuffer, Map}
import org.wquery.model._

class LmfLoader extends WordNetLoader with Logging {
  def canLoad(url: String) = { // TODO provide an XML parser based load check
    val input = new BufferedReader(new FileReader(url))
    val header = input.readLine() + input.readLine()

    input.close()
    header.contains("<LexicalResource")
  }

  override def load(url: String): WordNet = {
    val wordnet = new WordNet(new InMemoryWordNetStore)
    val factory = SAXParserFactory.newInstance

    factory.newSAXParser.parse(new File(url), new LmfHandler(wordnet))
    info("WordNet loaded via LmfLoader")
    wordnet
  }
}

class LmfHandler(wordNet: WordNet) extends DefaultHandler with Logging {
  // parser attributes
  private var locator: Locator = null

  // global attributes
  private val sensesBySynsetId = Map[String, ListBuffer[Sense]]()
  private val synsetsById = Map[String, Synset]()
  private val senseNumbers = Map[(String, String), Int]()
  private val synsetRelationsTuples = new ListBuffer[(String, String, String)]()
  private val stringRelationsTuples = new ListBuffer[(String, String, String)]()

  // per lemma attributes
  private var writtenForm: String = null
  private var partOfSpeech: String = null

  // per synset attributes
  private var synsetId: String = null

  override def setDocumentLocator(loc: Locator) { locator = loc }

  override def startElement(uri: String, localName: String, tagName: String, attributes: Attributes) {
    tagName match {
      case "Lemma" =>
        writtenForm = getAttributeOrWarn(tagName, attributes, "writtenForm", "skipping all related senses")
        partOfSpeech = getAttributeOrWarn(tagName, attributes, "partOfSpeech", "skipping all related senses")
      case "Sense" =>
        val senseSynsetId = getAttributeOrWarn(tagName, attributes, "synset", "skipping the related sense")
        val id = getAttributeOrWarn(tagName, attributes, "id", "skipping the related sense")

        if (writtenForm != null && partOfSpeech != null) {
          val senseNumber = senseNumbers.get((writtenForm, partOfSpeech)).getOrElse(0) + 1

          senseNumbers.put((writtenForm, partOfSpeech), senseNumber)

          if (id != null && senseSynsetId != null) {
            if (!sensesBySynsetId.contains(senseSynsetId))
              sensesBySynsetId(senseSynsetId) = new ListBuffer[Sense]()

            sensesBySynsetId(senseSynsetId).append(new Sense(id, writtenForm, senseNumber, partOfSpeech))
          }
        }
      case "Synset" =>
        synsetId = getAttributeOrWarn(tagName, attributes, "id", "(skipping the synset)")
        val bcs = attributes.getValue("baseConcept")

        if (synsetId != null && bcs != null)
          stringRelationsTuples.append((synsetId, "bcs", bcs))
      case "Definition" =>
        if (synsetId != null)
          stringRelationsTuples.append((synsetId, "gloss", attributes.getValue("gloss")))
      case "Statement" =>
        if (synsetId != null)
          stringRelationsTuples.append((synsetId, "example", attributes.getValue("example")))
      case "SynsetRelation" =>
        if (synsetId != null)
          synsetRelationsTuples.append((synsetId, attributes.getValue("relType"), attributes.getValue("target")))
      case _ =>
        // skip
    }
  }

  override def endDocument {
    createSynsets
    createSynsetRelations
    createStringRelations
  }

  private def createSynsets {
    for ((synsetId, senses) <- sensesBySynsetId) {
      val synset = new Synset(synsetId, senses.toList)
      synsetsById(synsetId) = synset
      wordNet.addSynset(synset)
    }
    info("Synsets loaded")
  }

  private def createSynsetRelations {
    for ((sourceSynsetId, relationName, destinationSynsetId) <- synsetRelationsTuples) {
      val relation = wordNet.getRelation(relationName, SynsetType, Relation.Source).getOrElse {
        wordNet.addRelation(Relation(relationName, SynsetType, SynsetType))
        wordNet.demandRelation(relationName, SynsetType, Relation.Source)
      }

      relation.destinationType.map { dt =>
        dt match {
          case SynsetType =>
            val sourceSynset = synsetsById.get(sourceSynsetId)
            val destinationSynset =  synsetsById.get(destinationSynsetId)

            if (sourceSynset == None)
              warn("Semantic relation '" + relation.name + "' points to an empty or unknown source synset '" + sourceSynsetId + "'")
            else if (destinationSynset == None)
              warn("Semantic relation '" + relation.name + "' points to an unknown destination synset '" + destinationSynsetId + "'")
            else
              wordNet.addSuccessor(sourceSynset.get, relation, destinationSynset.get)
          case destinationType =>
            throw new RuntimeException("Semantic relation '" + relation.name + "' has incorrect destination type " + destinationType)
        }
      }.getOrElse(throw new RuntimeException("Semantic relation '" + relation.name + "' has no destination type"))
    }

    info("Synset relations loaded")
  }

  private def createStringRelations {
    for ((sourceSynsetId, relationName, destination) <- stringRelationsTuples) {
      val relation = wordNet.getRelation(relationName, SynsetType, Relation.Source).getOrElse {
        wordNet.addRelation(Relation(relationName, SynsetType, StringType))
        wordNet.demandRelation(relationName, SynsetType, Relation.Source)
      }

      relation.destinationType.map { dt =>
        dt match {
          case StringType =>
            val sourceSynset = synsetsById.get(sourceSynsetId)

            if (sourceSynset == None)
              warn("Relation '" + relation.name + "' points to an empty or unknown source synset '" + sourceSynsetId + "'")
            else
              wordNet.addSuccessor(sourceSynset.get, relation, destination)
          case destinationType =>
            throw new RuntimeException("Relation '" + relation.name + "' has incorrect destination type " + destinationType)
        }
      }.getOrElse(throw new RuntimeException("Relation '" + relation.name + "' has no destination type"))
    }

    info("String relations loaded")
  }

  private def getAttributeOrWarn(tag: String, attributes: Attributes, name: String, note: String) = {
    val value = attributes.getValue(name)

    if (value == null)
      locatedWarn(tag + " has no '" + name + "' attribute (" + note + ")")

    value
  }

  private def locatedWarn(message: String)
    = warn("(" + locator.getLineNumber + ", " + locator.getColumnNumber + ") " + message)
}