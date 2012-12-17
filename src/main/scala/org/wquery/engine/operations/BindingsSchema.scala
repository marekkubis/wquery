package org.wquery.engine.operations

import org.wquery.model._

class BindingsSchema(val parent: Option[BindingsSchema], updatesParent: Boolean) extends BindingsPattern {
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

  override def bindSetVariableType(name: String, op: AlgebraOp) {
    if (updatesParent && parent.map(_.lookupPathVariableType(name).isDefined).getOrElse(false)) {
      parent.get.bindSetVariableType(name, op)
    } else {
      super.bindSetVariableType(name, op)
    }
  }

  override def lookupStepVariableType(name: String): Option[Set[DataType]] = {
    super.lookupStepVariableType(name)
      .orElse(parent.flatMap(_.lookupStepVariableType(name)))
  }

  override def lookupPathVariableType(name: String): Option[(AlgebraOp, Int, Int)] = {
    super.lookupPathVariableType(name)
      .orElse(parent.flatMap(_.lookupPathVariableType(name)))
  }

  override def lookupSetVariableType(name: String): Option[AlgebraOp] = {
    super.lookupSetVariableType(name)
      .orElse(parent.flatMap(_.lookupSetVariableType(name)))
  }

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
