package org.wquery.lang.operations

import org.wquery.model._

import scalaz.Scalaz._

class BindingsSchema(val parent: Option[BindingsSchema], updatesParent: Boolean) extends BindingsPattern {
  override def bindStepVariableType(name: String, types: Set[DataType]) {
    if (updatesParent && parent.some(_.lookupStepVariableType(name).isDefined).none(false)) {
      parent.get.bindStepVariableType(name, types)
    } else {
      super.bindStepVariableType(name, types)
    }
  }

  override def bindTupleVariableType(name: String, op: AlgebraOp, leftShift: Int, rightShift: Int) {
    if (updatesParent && parent.some(_.lookupTupleVariableType(name).isDefined).none(false)) {
      parent.get.bindTupleVariableType(name, op, leftShift, rightShift)
    } else {
      super.bindTupleVariableType(name, op, leftShift, rightShift)
    }
  }

  override def bindSetVariableType(name: String, op: AlgebraOp, leftShift: Int, keepTail: Boolean) {
    if (updatesParent && parent.some(_.lookupTupleVariableType(name).isDefined).none(false)) {
      parent.get.bindSetVariableType(name, op, leftShift, keepTail)
    } else {
      super.bindSetVariableType(name, op, leftShift, keepTail)
    }
  }

  override def lookupStepVariableType(name: String): Option[Set[DataType]] = {
    super.lookupStepVariableType(name)
      .orElse(parent.flatMap(_.lookupStepVariableType(name)))
  }

  override def lookupTupleVariableType(name: String): Option[(AlgebraOp, Int, Int)] = {
    super.lookupTupleVariableType(name)
      .orElse(parent.flatMap(_.lookupTupleVariableType(name)))
  }

  override def lookupSetVariableType(name: String): Option[(AlgebraOp, Int, Boolean)] = {
    super.lookupSetVariableType(name)
      .orElse(parent.flatMap(_.lookupSetVariableType(name)))
  }

  override def union(pattern: BindingsPattern) = {
    val sum = new BindingsSchema(parent, false)

    for ((name, types) <- stepVariablesTypes)
      sum.bindStepVariableType(name, types)

    for ((name, types) <- pattern.stepVariablesTypes)
      sum.bindStepVariableType(name, types)

    for ((name, (op, left, right)) <- tupleVariablesTypes)
      sum.bindTupleVariableType(name, op, left, right)

    for ((name, (op, left, right)) <- pattern.tupleVariablesTypes)
      sum.bindTupleVariableType(name, op, left, right)

    for ((name, (op, left, tail)) <- setVariablesTypes)
      sum.bindSetVariableType(name, op, left, tail)

    for ((name, (op, left, tail)) <- pattern.setVariablesTypes)
      sum.bindSetVariableType(name, op, left, tail)

    sum
  }
}

object BindingsSchema {
  def apply() = new BindingsSchema(None, false)

  def apply(parent: BindingsSchema, updatesParent: Boolean) = new BindingsSchema(Some(parent), updatesParent)
}
