package org.wquery.model

trait WordNetStore {
  def relations: List[Relation]

  def add(relation: Relation)

  def remove(relation: Relation)

  def add(relation: Relation, tuple: List[(String, Any)])

  def remove(relation: Relation, tuple: List[(String, Any)])

  def fetch(relation: Relation, from: List[(String, List[Any])], to: List[String]): DataSet

  def extend(dataSet: DataSet, pattern: ExtensionPattern): DataSet

}

case class ExtensionPattern(val pos: Int, val extensions: List[Extension])

case class Extension(val relation: Relation, val from: String, val to: List[String])