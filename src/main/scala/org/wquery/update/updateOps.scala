package org.wquery.update.operations

import scalaz._
import Scalaz._
import org.wquery.model._
import org.wquery.WQueryInvalidValueSpecifiedForRelationPropertyException
import scala.collection.mutable.{Map => MMap}
import org.wquery.lang.{Context, Variable}
import org.wquery.lang.operations._
import org.wquery.path.operations._
import org.wquery.query.operations._
import org.wquery.update._

sealed abstract class UpdateOp extends AlgebraOp {
  def update(wordNet: WordNet, bindings: Bindings, context: Context)

  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = {
    update(wordNet, bindings, context)
    DataSet.empty
  }

  def leftType(pos: Int) = Set.empty

  def rightType(pos: Int) = Set.empty

  val minTupleSize = 0

  val maxTupleSize = some(0)

  def bindingsPattern = BindingsSchema()

  def maxCount(wordNet: WordNet#Schema) = some(0)

}

case class AddTuplesOp(leftOp: Option[AlgebraOp], spec: RelationSpecification, rightOp: AlgebraOp) extends UpdateOp {
  def update(wordNet: WordNet, bindings: Bindings, context: Context) {
    val (relation, leftArgs, rightArgs) = spec.resolve(wordNet.schema, bindings, leftOp.map(_.minTupleSize).orZero)
    val opContext = if (WordNet.domainRelations.contains(relation)) context.copy(creation = true) else context
    val op = leftOp.some[AlgebraOp](JoinOp(_, rightOp)).none(rightOp)
    val dataSet = op.evaluate(wordNet, bindings, opContext)

    for (path <- dataSet.paths) {
      wordNet.addTuple(relation, (leftArgs ++ rightArgs).zip(path).toMap)
    }
  }

  val referencedVariables = rightOp.referencedVariables ++ leftOp.map(_.referencedVariables).orZero
}

case class RemoveTuplesOp(leftOp: Option[AlgebraOp], spec: RelationSpecification, rightOp: AlgebraOp) extends UpdateOp {
  def update(wordNet: WordNet, bindings: Bindings, context: Context) {
    val (relation, leftArgs, rightArgs) = spec.resolve(wordNet.schema, bindings, leftOp.map(_.minTupleSize).orZero)
    val op = leftOp.some[AlgebraOp](JoinOp(_, rightOp)).none(rightOp)
    val dataSet = op.evaluate(wordNet, bindings, context)

    for (path <- dataSet.paths) {
      wordNet.removeTuple(relation, (leftArgs ++ rightArgs).zip(path).toMap)
    }
  }

  val referencedVariables = rightOp.referencedVariables ++ leftOp.map(_.referencedVariables).orZero
}

case class SetTuplesOp(leftOp: Option[AlgebraOp], spec: RelationSpecification, rightOp:AlgebraOp) extends UpdateOp {
  def update(wordNet: WordNet, bindings: Bindings, context: Context) {
    val (relation, leftArgs, rightArgs) = spec.resolve(wordNet.schema, bindings, leftOp.map(_.minTupleSize).orZero)
    val leftPaths = leftOp.map(_.evaluate(wordNet, bindings, context).paths).orZero
    val rightContext = if (WordNet.domainRelations.contains(relation)) context.copy(creation = true) else context
    val rightPaths = rightOp.evaluate(wordNet, bindings, rightContext).paths

    // TODO check sizes

    wordNet.setTuples(relation, leftArgs, leftPaths, rightArgs, rightPaths)
  }

  val referencedVariables = rightOp.referencedVariables ++ leftOp.map(_.referencedVariables).orZero
}

case class MergeOp(valuesOp: AlgebraOp) extends UpdateOp {
  def update(wordNet: WordNet, bindings: Bindings, context: Context) {
    val (synsets, senses) = valuesOp.evaluate(wordNet, bindings, context).paths.map(_.last)
      .partition(_.isInstanceOf[Synset]).asInstanceOf[(List[Synset], List[Sense])]
    wordNet.merge(synsets, senses)
  }

  val referencedVariables = valuesOp.referencedVariables
}
