package org.wquery.model

trait WordNetStore {
  def relations: List[Relation]

  def add(relation: Relation)

  def remove(relation: Relation)

  def add(relation: Relation, tuple: List[(String, Any)])

  def remove(relation: Relation, tuple: List[(String, Any)])

  def generate(relation: Relation, from: List[(String, List[Any])], to: List[String]): DataSet

  def extend(dataSet: DataSet, relation: Relation, from: List[(Int, String)], to: List[String]): DataSet

}