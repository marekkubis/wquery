package org.wquery.engine

import org.wquery.model._

sealed abstract class UpdateOp extends AlgebraOp {
  def update(wordNet: WordNet, bindings: Bindings): Unit

  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    update(wordNet, bindings)
    DataSet.empty
  }

  def leftType(pos: Int) = Set.empty

  def rightType(pos: Int) = Set.empty

  def minTupleSize = 0

  def maxTupleSize = Some(0)

  def bindingsSchema = BindingsSchema()
}

//
// Add/Remove/Create ... Op
//

case class CreateRelationFromPatternOp(name: String, sourceType: NodeType, destinationType: NodeType, pattern: ExtensionPattern) extends UpdateOp {
  def update(wordNet: WordNet, bindings: Bindings) {
    wordNet.getRelation(name, Set(sourceType), Relation.Source).map(wordNet.removeRelation(_))

    val relation = Relation(name, sourceType, destinationType)

    wordNet.addRelation(relation)
    wordNet.store.add(relation, pattern)
  }
}