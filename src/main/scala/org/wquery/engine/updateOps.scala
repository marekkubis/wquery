package org.wquery.engine

import org.wquery.model._

sealed abstract class UpdateOp extends AlgebraOp {
  def update(wordNet: WordNet, bindings: Bindings)

  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    update(wordNet, bindings)
    DataSet.empty
  }

  def leftType(pos: Int) = Set.empty

  def rightType(pos: Int) = Set.empty

  def minTupleSize = 0

  def maxTupleSize = Some(0)

  def bindingsPattern = BindingsSchema()
}

//
// Add/Remove/Create ... Op
//

case class CreateRelationFromPatternOp(name: String, pattern: RelationalPattern, sourceType: NodeType, destinationType: NodeType) extends UpdateOp {
  def update(wordNet: WordNet, bindings: Bindings) {
    wordNet.schema.getRelation(name, Map((Relation.Source, pattern.sourceType))).map(wordNet.store.remove(_))

    val relation = Relation(name, sourceType, destinationType)

    wordNet.store.add(relation)
    wordNet.store.add(relation, pattern)
  }
}