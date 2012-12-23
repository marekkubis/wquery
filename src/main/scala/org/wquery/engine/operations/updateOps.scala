package org.wquery.engine.operations

import scalaz._
import Scalaz._
import org.wquery.model._
import org.wquery.WQueryInvalidValueSpecifiedForRelationPropertyException
import scala.collection.mutable.{Map => MMap}
import org.wquery.engine.{Context, Variable}

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

  def maxCount(wordNet: WordNetSchema) = some(0)

  def cost(wordNet: WordNetSchema) = some(1) // TODO implement cost model for update operations
}

//
// Add/Set/Remove ... Op
//

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

sealed abstract class WordNetUpdateWithAssignmentsOp[A](val valuesOp: AlgebraOp) extends UpdateOp {
  val referencedVariables = valuesOp.referencedVariables

  def update(wordNet: WordNet, bindings: Bindings, context: Context) {
    updateWithAssignments(wordNet, bindings, context)
  }

  def updateWithAssignments(wordNet: WordNet, bindings: Bindings, context: Context)
}

sealed abstract class WordNetUpdateWithValuesOp[A](override val valuesOp: AlgebraOp) extends WordNetUpdateWithAssignmentsOp(valuesOp) {
  def updateWithAssignments(wordNet: WordNet, bindings: Bindings, context: Context) {
    valuesOp.evaluate(wordNet, bindings, context).paths.foreach(tuple => updateWithValue(wordNet, bindings, tuple.last.asInstanceOf[A]))
  }

  def updateWithValue(wordNet: WordNet, bindings: Bindings, value: A)
}

case class AddRelationsOp(override val valuesOp: AlgebraOp) extends WordNetUpdateWithAssignmentsOp(valuesOp) {
  def updateWithAssignments(wordNet: WordNet, bindings: Bindings, context: Context) = {
    val valueSet = valuesOp.evaluate(wordNet, bindings, context)
    valueSet.paths.map(_.last.asInstanceOf[Arc].relation).distinct.foreach(wordNet.addRelation(_))
  }
}

case class RemoveRelationsOp(override val valuesOp: AlgebraOp) extends WordNetUpdateWithAssignmentsOp(valuesOp) {
  def updateWithAssignments(wordNet: WordNet, bindings: Bindings, context: Context) = {
    val valueSet = valuesOp.evaluate(wordNet, bindings, context)
    valueSet.paths.map(_.last.asInstanceOf[Arc].relation).distinct.foreach(wordNet.removeRelation(_))
  }
}

case class SetRelationsOp(override val valuesOp: AlgebraOp) extends WordNetUpdateWithAssignmentsOp(valuesOp) {
  def updateWithAssignments(wordNet: WordNet, bindings: Bindings, context: Context) = {
    val valueSet = valuesOp.evaluate(wordNet, bindings, context)
    val newRelations = valueSet.paths.map(_.last.asInstanceOf[Arc].relation).distinct

    wordNet.setRelations(newRelations)
  }
}

case class RelationUpdateOp(arcsOp: AlgebraOp, op: String, property: String, action: Boolean, valuesOp: AlgebraOp) extends UpdateOp {
  val referencedVariables = arcsOp.referencedVariables ++ valuesOp.referencedVariables

  def update(wordNet: WordNet, bindings: Bindings, context: Context) = {
    val arcs = arcsOp.evaluate(wordNet, bindings, context).paths.map(_.last.asInstanceOf[Arc])
    val value = valuesOp.evaluate(wordNet, bindings, context).paths.map(_.last).head

    if (action) {
      if (Relation.PropertyActions.contains(value)) {
        property match {
          case Relation.Transitivity =>
            updateRelationPropertyAction(wordNet.transitivesActions, arcs, value)
          case Relation.Symmetry =>
            updateRelationPropertyAction(wordNet.symmetryActions, arcs, value)
          case Relation.Functional =>
            updateRelationArgumentPropertyAction(wordNet.functionalForActions, arcs, value)
        }
      } else {
        throw new WQueryInvalidValueSpecifiedForRelationPropertyException(property)
      }
    } else {
      property match {
        case Relation.Transitivity =>
          updateTransitivityProperty(wordNet, arcs, value)
        case Relation.Symmetry =>
          updateSymmetryProperty(wordNet, arcs, value)
        case Relation.RequiredBy =>
          updateRelationArgumentProperty(wordNet.requiredBys, arcs, value)
        case Relation.Functional =>
          updateRelationArgumentProperty(wordNet.functionalFor, arcs, value)
      }
    }
  }

  private def updateTransitivityProperty(wordNet: WordNet, arcs: List[Arc], value: Any) {
    val logicValue = value.asInstanceOf[Boolean]

    if (op == "+=" || op == ":=") {
      for (arc <- arcs)
        wordNet.transitives(arc.relation) = logicValue
    } else {
      for (arc <- arcs) {
        if (wordNet.transitives(arc.relation) == value)
          wordNet.transitives(arc.relation) = !logicValue
      }
    }
  }

  private def updateSymmetryProperty(wordNet: WordNet, arcs: List[Arc], value: Any) {
    val symmetry = value.asInstanceOf[String] match {
      case "antisymmetric" =>
        Antisymmetric
      case "symmetric" =>
        Symmetric
      case "nonsymmetric" =>
        NonSymmetric
      case _ =>
        throw new WQueryInvalidValueSpecifiedForRelationPropertyException(Relation.Symmetry)
    }

    if (op == "+=" || op == ":=") {
      for (arc <- arcs)
        wordNet.symmetry(arc.relation) = symmetry
    } else {
      for (arc <- arcs) {
        if (wordNet.symmetry(arc.relation) == symmetry)
          wordNet.symmetry(arc.relation) = NonSymmetric
      }
    }
  }

  private def updateRelationArgumentProperty(propertyCollection: MMap[Relation, Set[String]], arcs: List[Arc], value: Any) {
    val logicValue = value.asInstanceOf[Boolean]

    if (op == "+=" || op == ":=") {
      if (logicValue) {
        for (arc <- arcs)
          propertyCollection(arc.relation) = propertyCollection(arc.relation) + arc.to
      } else {
        for (arc <- arcs)
          propertyCollection(arc.relation) = propertyCollection(arc.relation) - arc.to
      }
    } else {
      if (logicValue) {
        for (arc <- arcs)
          propertyCollection(arc.relation) = propertyCollection(arc.relation) - arc.to
      } else {
        for (arc <- arcs)
          propertyCollection(arc.relation) = propertyCollection(arc.relation) + arc.to
      }
    }
  }

  private def updateRelationPropertyAction(actionCollection: MMap[Relation, String], arcs: List[Arc], value: Any) {
    val action = value.asInstanceOf[String]

    if (op == "+=" || op == ":=") {
      for (arc <- arcs)
        actionCollection(arc.relation) = action
    } else {
      for (arc <- arcs) {
        if (actionCollection(arc.relation) == action)
          actionCollection(arc.relation) = if (action == Relation.Restore) Relation.Preserve else Relation.Restore
      }
    }
  }

  private def updateRelationArgumentPropertyAction(actionCollection: MMap[(Relation, String), String], arcs: List[Arc], value: Any) {
    val action = value.asInstanceOf[String]

    if (op == "+=" || op == ":=") {
      for (arc <- arcs)
        actionCollection((arc.relation, arc.to)) = action
    } else {
      for (arc <- arcs) {
        if (actionCollection((arc.relation, arc.to)) == action)
          actionCollection((arc.relation, arc.to)) = if (action == Relation.Restore) Relation.Preserve else Relation.Restore
      }
    }
  }
}

//
// Merge
//

case class MergeOp(override val valuesOp: AlgebraOp) extends WordNetUpdateWithAssignmentsOp(valuesOp) {
  def updateWithAssignments(wordNet: WordNet, bindings: Bindings, context: Context) = {
    val (synsets, senses) = valuesOp.evaluate(wordNet, bindings, context).paths.map(_.last)
      .partition(_.isInstanceOf[Synset]).asInstanceOf[(List[Synset], List[Sense])]
    wordNet.merge(synsets, senses)
  }
}
