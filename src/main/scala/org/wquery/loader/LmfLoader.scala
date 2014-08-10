// scalastyle:off null
// scalastyle:off multiple.string.literals

package org.wquery.loader

import java.io.InputStream
import javax.xml.parsers.SAXParserFactory

import org.wquery.model._
import org.wquery.model.impl.InMemoryWordNet
import org.wquery.utils.Logging
import org.xml.sax.helpers.DefaultHandler
import org.xml.sax.{Attributes, Locator}

import scala.collection.immutable.{Map => IMap}
import scala.collection.mutable.{ListBuffer, Map}
import scalaz.Scalaz._

class LmfLoader extends WordNetLoader with Logging {
  override def load(input: InputStream) = {
    val factory = SAXParserFactory.newInstance
    val wordNet = new InMemoryWordNet

    factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false)
    factory.newSAXParser.parse(input, new LmfHandler(wordNet))
    info("WordNet loaded via LmfLoader")
    wordNet
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
  private var writtenForm = none[String]
  private var partOfSpeech = none[String]

  // per synset attributes
  private var synsetIdOption = none[String]

  override def setDocumentLocator(loc: Locator) { locator = loc }

  override def startElement(uri: String, localName: String, tagName: String, attributes: Attributes) {
    tagName match {
      case "Lemma" =>
        writtenForm = getAttributeOrWarn(tagName, attributes, "writtenForm", "skipping all related senses")
        partOfSpeech = getAttributeOrWarn(tagName, attributes, "partOfSpeech", "skipping all related senses")
      case "Sense" =>
        startSenseElement(attributes)
      case "Synset" =>
        startSynsetElement(attributes)
      case "Definition" =>
        synsetIdOption.map(id => stringRelationsTuples.append((id, "gloss", attributes.getValue("gloss"))))
      case "Statement" =>
        synsetIdOption.map(id => stringRelationsTuples.append((id, "example", attributes.getValue("example"))))
      case "SynsetRelation" =>
        synsetIdOption.map(id => synsetRelationsTuples.append((id, attributes.getValue("relType"), attributes.getValue("target"))))
      case _ =>
        // skip
    }
  }

  private def startSenseElement(attributes: Attributes) {
    val senseSynsetId = getAttributeOrWarn("Sense", attributes, "synset", "skipping the related sense")

    (writtenForm <|*|> partOfSpeech).map{ case (form, pos) =>
      val senseNumber = senseNumbers.getOrElse((form, pos), 0) + 1

      senseNumbers.put((form, pos), senseNumber)

      senseSynsetId.map { id =>
        if (!sensesBySynsetId.contains(id))
          sensesBySynsetId(id) = new ListBuffer[Sense]()

        sensesBySynsetId(id).append(new Sense(form, senseNumber, pos))
      }
    }
  }

  private def startSynsetElement(attributes: Attributes) {
    synsetIdOption = getAttributeOrWarn("Synset", attributes, "id", "(skipping the synset)")
    val bcs = attributes.getValue("baseConcept")

    if (synsetIdOption.isDefined && bcs != null)
      stringRelationsTuples.append((synsetIdOption.get, "bcs", bcs))
  }

  override def endDocument {
    createSynsets
    createSynsetRelations
    createStringRelations
  }

  private def createSynsets {
    for ((synsetId, senses) <- sensesBySynsetId) {
      synsetsById(synsetId) = wordNet.addSynset(Some(synsetId), senses.toList, moveSenses = false)
    }
    info("Synsets loaded")
  }

  private def createSynsetRelations {
    for ((sourceSynsetId, relationName, destinationSynsetId) <- synsetRelationsTuples) {
      val relation = wordNet.schema.getRelation(relationName, IMap((Relation.Src, Set[DataType](SynsetType))))|{
        wordNet.addRelation(Relation.binary(relationName, SynsetType, SynsetType))
        wordNet.schema.demandRelation(relationName, IMap((Relation.Src, Set[DataType](SynsetType))))
      }

      relation.destinationType.some { dt =>
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
      }.none(throw new RuntimeException("Semantic relation '" + relation.name + "' has no destination type"))
    }

    info("Synset relations loaded")
  }

  private def createStringRelations {
    for ((sourceSynsetId, relationName, destination) <- stringRelationsTuples) {
      val relation = wordNet.schema.getRelation(relationName, IMap((Relation.Src, Set[DataType](SynsetType))))| {
        wordNet.addRelation(Relation.binary(relationName, SynsetType, StringType))
        wordNet.schema.demandRelation(relationName, IMap((Relation.Src, Set[DataType](SynsetType))))
      }

      relation.destinationType.some { dt =>
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
      }.none(throw new RuntimeException("Relation '" + relation.name + "' has no destination type"))
    }

    info("String relations loaded")
  }

  private def getAttributeOrWarn(tag: String, attributes: Attributes, name: String, note: String) = {
    val value = attributes.getValue(name)

    if (value == null) {
      locatedWarn(tag + " has no '" + name + "' attribute (" + note + ")")
      none[String]
    } else {
      some(value)
    }
  }

  private def locatedWarn(message: String)
    = warn("(" + locator.getLineNumber + ", " + locator.getColumnNumber + ") " + message)
}
