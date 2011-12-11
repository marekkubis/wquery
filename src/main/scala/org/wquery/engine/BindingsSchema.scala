package org.wquery.engine

import org.wquery.model._
import org.wquery.WQueryEvaluationException

class BindingsSchema(val parent: Option[BindingsSchema], updatesParent: Boolean) extends BindingsPattern {
  private var contextOp: Option[AlgebraOp] = None

  override def bindStepVariableType(name: String, types: Set[DataType]) {
    if (updatesParent && parent.map(_.lookupStepVariableType(name).isDefined).getOrElse(false)) {
      parent.get.bindStepVariableType(name, types)
    } else {
      super.bindStepVariableType(name, types)
    }
  }

  override def bindPathVariableType(name: String, op: AlgebraOp, leftShift: Int, rightShift: Int) {
    if (updatesParent && parent.map(_.lookupPathVariableType(name).isDefined).getOrElse(false)) {
      parent.get.bindPathVariableType(name, op, leftShift, rightShift)
    } else {
      super.bindPathVariableType(name, op, leftShift, rightShift)
    }
  }

  def bindContextOp(op: AlgebraOp) = contextOp = Some(op)

  override def lookupStepVariableType(name: String): Option[Set[DataType]] = {
    super.lookupStepVariableType(name)
      .orElse(parent.flatMap(_.lookupStepVariableType(name)))
      .orElse(contextOp.map(op => op.bindingsPattern.lookupStepVariableType(name)).getOrElse(None))
  }

  override def lookupPathVariableType(name: String): Option[(AlgebraOp, Int, Int)] = {
    super.lookupPathVariableType(name)
      .orElse(parent.flatMap(_.lookupPathVariableType(name)))
      .orElse(contextOp.map(op => op.bindingsPattern.lookupPathVariableType(name)).getOrElse(None))
  }

  def lookupContextVariableType(pos: Int): Set[DataType] = {
    contextOp.map(_.rightType(pos))
      .getOrElse(parent.map(_.lookupContextVariableType(pos))
        .getOrElse{throw new WQueryEvaluationException("Operator # placed outside of a filter")})
  }

  def areContextVariablesBound: Boolean = contextOp.isDefined || parent.map(_.areContextVariablesBound).getOrElse(false)

  override def union(pattern: BindingsPattern) = {
    val sum = new BindingsSchema(parent, false)

    for ((name, types) <- stepVariablesTypes)
      sum.bindStepVariableType(name, types)

    for ((name, types) <- pattern.stepVariablesTypes)
      sum.bindStepVariableType(name, types)

    for ((name, (op, left, right)) <- pathVariablesTypes)
      sum.bindPathVariableType(name, op, left, right)

    for ((name, (op, left, right)) <- pattern.pathVariablesTypes)
      sum.bindPathVariableType(name, op, left, right)

    sum
  }
}

object BindingsSchema {
  def apply() = new BindingsSchema(None, false)
  
  def apply(parent: BindingsSchema, updatesParent: Boolean) = new BindingsSchema(Some(parent), updatesParent)
}