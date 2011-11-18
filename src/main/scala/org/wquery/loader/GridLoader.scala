package org.wquery.loader
import javax.xml.parsers.SAXParserFactory
import java.io.File
import org.wquery.utils.Logging
import org.xml.sax.{Locator, Attributes}
import org.xml.sax.helpers.DefaultHandler
import scala.collection.mutable.{Set, Map, ListBuffer}
import org.wquery.model._
import org.wquery.WQueryUpdateBreaksRelationPropertyException

class GridLoader extends WordNetLoader with Logging {
  override def canLoad(url: String): Boolean = url.endsWith(".xml") // TODO provide a better check

  override def load(url: String, wordNet: WordNet) = {
    val factory = SAXParserFactory.newInstance

    factory.newSAXParser.parse(new File(url), new GridHandler(wordNet))
    info("WordNet loaded via GridLoader")
  }

}

class GridHandler(wordnet: WordNet) extends DefaultHandler with Logging {
  // parser attributes
  private var locator: Locator = null
  
  // global attributes
  private val synsetsById = Map[String, Synset]()
  private val ilrRelationsTuples = new ListBuffer[(Synset, String, String)]()    
  private val genericRelationsTuples = new ListBuffer[(Synset, String, String)]()
  
  // per synset attributes
  private var synsetId: String = null
  private var synsetPos: String = null
  private var synsetIlrRelationsTuples: ListBuffer[(String, String)] = null    
  private var synsetGenericRelationsTuples: ListBuffer[(String, String)] = null
  private var synsetSenses: ListBuffer[(String, String)] = null 
  
  // per subtag attributes
  private var literalSense: String = null  
  private var ilrType: String = null
  private var text = new StringBuilder
  private var previousText: StringBuilder = null

  override def setDocumentLocator(loc: Locator) { locator = loc } 
    
  override def startElement(uri: String, localName: String, tagName: String, attributes: Attributes) {
    pushText       
        
    tagName match {
      case "SYNSET" =>
        synsetId = null
        synsetPos = null    
        synsetIlrRelationsTuples = new ListBuffer[(String, String)]    
        synsetGenericRelationsTuples = new ListBuffer[(String, String)]        
        synsetSenses = new ListBuffer[(String, String)]        
      case "LITERAL" =>
        literalSense = attributes.getValue("sense")
      case "ILR" =>
      ilrType = attributes.getValue("type")
      case _ =>
        // skip
    }    
  }
    
  override def characters(chars: Array[Char], start: Int, length: Int) {      
    text.appendAll(chars, start, length)
  }

  override def endElement(uri: String, localName: String, tagName: String) {
    tagName match {
      case "SYNSET" =>
        endSynsetTag
      case tagName =>
        val content = text.toString.trim
        
        tagName match {
          case "ID" =>
            synsetId = content
          case "POS" =>
            synsetPos = content            
            endGenericTag("POS", content)            
          case "LITERAL" =>
            synsetSenses += ((content, literalSense))
            literalSense = null
          case "SENSE" =>
            literalSense = content
          case "ILR" =>
            synsetIlrRelationsTuples += ((ilrType, content))    
            ilrType = null
          case "TYPE" =>
            ilrType = content
          case tagName =>
            endGenericTag(tagName, content)          
        }
    }
      
    tagName match {
      case "SENSE" =>
        popText
      case _ =>
        pushText
    }        
  }

  private def pushText {
    previousText = text
    text = new StringBuilder        
  }
    
  private def popText {
    text = previousText
  }    
  
  private def endSynsetTag {    
    if (synsetId == null) {
      locatedWarn("SYNSET tag does not contain ID subtag")
    } else {
      if (synsetPos == null) {
        locatedWarn("SYNSET '" + synsetId + "' does not contain POS tag - 'x' symbol used as the POS tag value")
        synsetPos = "x"
      }
      
      val senses = new ListBuffer[Sense]
      
      synsetSenses.toList.zipWithIndex.foreach {case ((content, literalSense), index) =>
        try {
          if (literalSense != null) {
            senses += new Sense(content, literalSense.toInt, synsetPos)
          } else {
            warnInvalidSubtag("LITERAL", content, "sense")
          }
        } catch {
          case ex: NumberFormatException =>
            warnInvalidSubtag("LITERAL", content, "sense") 
        }
      }

      val synset = wordnet.store.addSynset(Some(synsetId), senses.toList, Nil, moveSenses = false)
      synsetsById(synset.id) = synset
      
      for ((ilrType, content) <- synsetIlrRelationsTuples) {
        if (ilrType == null || ilrType.trim.isEmpty) {
          warnInvalidSubtag("ILR", content, "type")      
        } else {
          ilrRelationsTuples += ((synset, ilrType.trim, content))
        }        
      }
      
      for ((tagName, content) <- synsetGenericRelationsTuples) {
        genericRelationsTuples += ((synset, tagName, content))
      }      
    }
  }  

  private def endGenericTag(tagName: String, content: String) {
    tagName.toLowerCase match {
      case "synonym" =>
        // skip
      case "word" =>
        // skip
      case relName =>
        synsetGenericRelationsTuples += ((relName, content))
    }
  }
    
  override def endDocument {
    info("Synsets loaded") 
    createIlrRelations    
    createGenericRelations 
  }
    
  private def createIlrRelations {
    // create semantic relations names
    val ilrRelationsNames = Set[String]()
    
    for ((synset, relname, reldest) <- ilrRelationsTuples) {
      ilrRelationsNames += relname
    }

    // create semantic relations
    for (name <- ilrRelationsNames) {
      val relation = Relation.binary(name, SynsetType, SynsetType)
      wordnet.store.addRelation(relation)

      //TODO move to configuration

      if (Set("hypernym", "partial_meronym", "member_meronym").contains(name)) {
        wordnet.store.transitives(relation) = true
        wordnet.store.transitivesActions(relation) = Relation.Restore
      }

      if (Set("hypernym").contains(name)) {
        wordnet.store.symmetry(relation) = Antisymmetric
        wordnet.store.symmetryActions(relation) = Relation.Preserve
      }

      if (Set("similar").contains(name)) {
        wordnet.store.symmetry(relation) = Symmetric
        wordnet.store.symmetryActions(relation) = Relation.Restore
      }
    }

    // create semantic relations successors       
    for ((synset, relname, reldest) <- ilrRelationsTuples) {      
      val relation = wordnet.schema.demandRelation(relname, scala.collection.immutable.Map((Relation.Source, scala.collection.immutable.Set[DataType](SynsetType))))

      relation.destinationType.map { dt =>
        dt match {
          case SynsetType =>
            synsetsById.get(reldest).map { destination =>
              try {
                wordnet.store.addSuccessor(synset, relation, destination)
              } catch {
                case e: WQueryUpdateBreaksRelationPropertyException =>
                  warn("ILR tag with type '" + relname + "' found in synset '" + synset.id + "' that points to synset '" + reldest + "' breaks property '" + e.property + "' of relation '" + e.relation.name + "'")
              }
            }.getOrElse(warn("ILR tag with type '" + relname + "' found in synset '" + synset.id + "' points to unknown synset '" + reldest + "'"))
          case dtype =>
            throw new RuntimeException("ILR tag points to relation " + relation + " that has incorrect destination type " + dtype)
        }
      }.getOrElse(throw new RuntimeException("Relation '" + relname + "' has no destination type"))

    }
    
    info("ILR relations loaded")    
  }
  
  private def createGenericRelations {    
    // create relations datatypes
    val genericRelationsDestTypes = Map[String, NodeType]()    
    
    for ((synset, relname, reldest) <- genericRelationsTuples) {
      val dtype = getType(reldest)
      
      if (!genericRelationsDestTypes.contains(relname) || 
            rankType(genericRelationsDestTypes(relname)) < rankType(dtype)) {
        genericRelationsDestTypes(relname) = dtype          
      }      
    }    

    // create relations
    for ((relname, dtype) <- genericRelationsDestTypes) {
      val relation = Relation.binary(relname, SynsetType, dtype)
      wordnet.store.addRelation(relation)

      if (relname == "desc") {
        wordnet.store.requiredBys(relation) = scala.collection.immutable.Set(Relation.Source)
        wordnet.store.functionalFor(relation) = scala.collection.immutable.Set(Relation.Source)
        wordnet.store.functionalForActions((relation, Relation.Source)) = Relation.Preserve
      }
    }
    
    // create successors
    for ((synset, relname, reldest) <- genericRelationsTuples) {
      val relation = wordnet.schema.demandRelation(relname, scala.collection.immutable.Map((Relation.Source, scala.collection.immutable.Set[DataType](SynsetType))))

      relation.destinationType.map { dt =>
        dt match {
          case SynsetType =>
            wordnet.store.addSuccessor(synset, relation, synsetsById(reldest))
          case BooleanType =>
            wordnet.store.addSuccessor(synset, relation, reldest.toBoolean)
          case IntegerType =>
            wordnet.store.addSuccessor(synset, relation, reldest.toInt)
          case FloatType =>
            wordnet.store.addSuccessor(synset, relation, reldest.toFloat)
          case StringType =>
            wordnet.store.addSuccessor(synset, relation, reldest)
          case dtype =>
            throw new RuntimeException("Incorrect destination type " + dtype + " as a successor of relation '" + relation + "'")
        }
      }.getOrElse(throw new RuntimeException("Relation '" + relname + "' has no destination type"))
    }

    info("non-ILR relations loaded")    
  }
    
  private def getType(destination: String): NodeType = {
    synsetsById.get(destination).map(_ => SynsetType).getOrElse(
      try {
        destination.toBoolean
        BooleanType
      } catch {
        case _:NumberFormatException =>
          try {
            destination.toInt
            IntegerType
          } catch {
            case _:NumberFormatException =>
              try {
                destination.toDouble
                FloatType
              } catch {
                case _:NumberFormatException => StringType
              }
          }
      }
    )
  }

  private def rankType(dtype: NodeType) = dtype match {
    case SynsetType => 0
    case SenseType => 1
    case BooleanType => 2
    case IntegerType => 3
    case FloatType => 4
    case POSType => 5
    case StringType => 6
  } 
  
  private def warnInvalidSubtag(tag: String, content: String, attr: String) 
    = locatedWarn(tag + " tag with content '" + content + "' found in synset '" + synsetId + 
                    "' does not have valid " + attr.toUpperCase + " subtag" +
                    " nor '" + attr.toUpperCase + "' attribute")
    
  private def locatedWarn(message: String) 
    = warn("(" + locator.getLineNumber + ", " + locator.getColumnNumber + ") " + message)
}
