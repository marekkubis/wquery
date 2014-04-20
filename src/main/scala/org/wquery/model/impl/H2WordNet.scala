package org.wquery.model.impl

import scalaz._
import Scalaz._
import org.wquery.model._
import org.wquery.utils.Logging
import scalikejdbc._, SQLInterpolation._

class H2WordNet extends WordNet with Logging {

  val dbTypes = Map[NodeType, String](SynsetType -> "varchar(64)", SenseType -> "int",
    StringType -> "varchar", POSType -> "varchar(16)", IntegerType -> "int",
    FloatType -> "double", BooleanType -> "boolean")

  def open(url: String) {
    Class.forName("org.h2.Driver")
    ConnectionPool.singleton("jdbc:h2:" + url + ";MAX_COMPACT_TIME=10000", "wordnet", "wordnet", ConnectionPoolSettings(maxSize = 1))
    setupSchema()
  }

  def close() {
    DB autoCommit { implicit session =>
      sql"shutdown compact".execute()
    }

    ConnectionPool.close()
  }

  def setupSchema() {
    DB localTx { implicit session =>
      sql"""
        create table if not exists senses(
          id varchar,
          word_form varchar,
          sense_num int,
          pos varchar(16),
          synset_id varchar(64)
        )""".update().apply()

      sql"""
        create table if not exists synsets(
          id varchar(64)
        )""".update().apply()

      sql"""
        create table if not exists relations(
          name varchar(64),
          src_type varchar(7),
          dst_type varchar(7)
        )""".update().apply()
    }
  }

  def addSynset(synsetId: Option[String], senses: List[Sense], moveSenses: Boolean = true) = {
    if (moveSenses)
      throw new IllegalArgumentException("Not implemented yet")

    val synset = new Synset(synsetId|("synset#" + senses.head))

    DB localTx { implicit session =>
      sql"""
          insert into synsets(id) values (${synset.id})
      """.update().apply()

      for (sense <- senses) {
        sql"""
          insert into senses(id, word_form, sense_num, pos, synset_id)
          values (${sense.toString}, ${sense.wordForm}, ${sense.senseNumber}, ${sense.pos}, ${synset.id})
        """.update().apply()
      }
    }

    synset
  }

  def addRelation(relation: Relation) {
    DB localTx { implicit session =>
      SQL("""
        create table if not exists """ + relation.name + """(
          id int primary key auto_increment,
          src """ + dbTypes(relation.sourceType) + """,
          dst """ + dbTypes(relation.destinationType.get) + """
        )
      """).update().apply()

      sql"""
          insert into relations(name, src_type, dst_type)
          values (${relation.name}, ${relation.sourceType.toString}, ${relation.destinationType.get.toString})
      """.update().apply()
    }
  }

  def addTuple(relation: Relation, tuple: Map[String, Any]) = {
    DB localTx { implicit session =>
      SQL("insert into " + relation.name + "(src, dst) values({src}, {dst})")
        .bindByName('src -> getKey(tuple("src")), 'dst -> getKey(tuple("dst")))
        .update().apply()
    }
  }

  def getKey(value: Any) = value match {
    case synset: Synset =>
      synset.id
    case sense: Sense =>
      sense.toString
    case str: String =>
      str
    case inum:Int =>
      inum
    case dnum:Double =>
      dnum
    case bool:Boolean =>
      bool
    case obj =>
      throw new IllegalArgumentException("Object " + obj + " has no data type bound")
  }

  override def relations = {
    DB readOnly { implicit session =>
      sql"""
          select name, src_type, dst_type from relations
      """.map{ rs =>
        Relation.binary(rs.string("name"), NodeType.fromName(rs.string("src_type")),
            NodeType.fromName(rs.string("dst_type")))
      }.list().apply()
    }
  }

  override def aliases = {
    // TODO do something
    Nil
  }

  override def merge(synsets: List[Synset], senses: List[Sense]) = ???

  override def setTuples(relation: Relation, sourceNames: List[String], sourceValues: List[List[Any]], destinationNames: List[String], destinationValues: List[List[Any]]) = ???

  override def removeTuple(relation: Relation, tuple: Map[String, Any], withDependentNodes: Boolean, withCollectionDependentNodes: Boolean) = ???

  override def setRelations(newRelations: List[Relation]) = ???

  override def removeRelation(relation: Relation) = ???

  override def setWords(newWords: List[String]) = ???

  override def setPartOfSpeechSymbols(newPartOfSpeechSymbols: List[String]) = ???

  override def setSenses(newSenses: List[Sense]) = ???

  override def setSynsets(synsets: List[Synset]) = ???

  override def removePartOfSpeechSymbol(pos: String) = ???

  override def removeWord(word: String) = ???

  override def removeSynset(synset: Synset) = ???

  override def removeSense(sense: Sense) = ???

  override def getSynset(sense: Sense) = ???

  override def getSenses(synset: Synset) = ???

  override def extend(extensionSet: ExtensionSet, through: (String, Option[NodeType]), to: (String, Option[NodeType])) = ???

  override def extend(extensionSet: ExtensionSet, relation: Relation, through: String, to: String) = ???

  override def fetch(relation: Relation, from: List[(String, List[Any])], to: List[String], withArcs: Boolean) = ???
}
