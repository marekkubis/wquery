package org.wquery.model

import org.wquery.engine.RelationalPattern

trait WordNetStore {
  def relations: List[Relation]

  def add(relation: Relation): Unit

  def add(relation: Relation, pattern: RelationalPattern): Unit

  def add(relation: Relation, tuple: List[(String, Any)]): Unit

  def remove(relation: Relation): Unit

  def fetch(relation: Relation, from: List[(String, List[Any])], to: List[String]): DataSet

  def extend(dataSet: DataSet, relation: Relation, from: Int, through: String, to: List[String]): DataSet
}
