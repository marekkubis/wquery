package org.wquery.model

trait WordNetStore {

  def relations: List[Relation]

  def add(relation: Relation): Unit

  def add(relation: Relation, pattern: ExtensionPattern): Unit

  def add(relation: Relation, tuple: List[(String, Any)]): Unit

  def remove(relation: Relation): Unit

  def fetch(relation: Relation, from: List[(String, List[Any])], to: List[String]): DataSet

  def extend(dataSet: DataSet, pattern: ExtensionPattern): DataSet

}
