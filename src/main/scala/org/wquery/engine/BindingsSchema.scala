package org.wquery.engine

import java.lang.reflect.Method
import scala.collection.mutable.Map
import org.wquery.model._
import org.wquery.WQueryEvaluationException

class BindingsSchema(val parent: Option[BindingsSchema], updatesParent: Boolean) {
  val stepVariablesTypes = Map[String, Set[DataType]]()
  val pathVariablesTypes = Map[String, (AlgebraOp, Int, Int)]()
  private var contextOp: Option[AlgebraOp] = None

  def bindStepVariableType(name: String, types: Set[DataType]) {
    if (updatesParent && parent.map(_.lookupStepVariableType(name).isDefined).getOrElse(false)) {
      parent.get.bindStepVariableType(name, types)
    } else {
      stepVariablesTypes(name) = types
    }
  }

  def bindPathVariableType(name: String, op: AlgebraOp, leftShift: Int, rightShift: Int) {
    if (updatesParent && parent.map(_.lookupPathVariableType(name).isDefined).getOrElse(false)) {
      parent.get.bindPathVariableType(name, op, leftShift, rightShift)
    } else {
      pathVariablesTypes(name) = (op, leftShift, rightShift)
    }
  }

  def bindContextOp(op: AlgebraOp) = contextOp = Some(op)

  def lookupStepVariableType(name: String): Option[Set[DataType]] = {
    stepVariablesTypes.get(name).orElse(parent.flatMap(_.lookupStepVariableType(name)))
  }

  def lookupPathVariableType(name: String): Option[(AlgebraOp, Int, Int)] = pathVariablesTypes.get(name).orElse(parent.flatMap(_.lookupPathVariableType(name)))

  def lookupContextVariableType(pos: Int): Set[DataType] = {
    contextOp.map(_.rightType(pos))
      .getOrElse(parent.map(_.lookupContextVariableType(pos))
        .getOrElse{throw new WQueryEvaluationException("Operator # placed outside of a context")})
  }

  def areContextVariablesBound: Boolean = contextOp.isDefined || parent.map(_.areContextVariablesBound).getOrElse(false)

  def union(that: BindingsSchema) = {
    val sum = parent match {
      case Some(parent) =>
        that.parent.map(throw new RuntimeException("An attempt to merge two bindings with defined parents"))
          .getOrElse(BindingsSchema(parent, false))
      case None =>
        that.parent.map(BindingsSchema(_, false)).getOrElse(BindingsSchema())
    }

    // TODO ugly loops
    for ((k, v) <- stepVariablesTypes)
      sum.bindStepVariableType(k, v)

    for ((k, v) <- that.stepVariablesTypes)
      sum.bindStepVariableType(k, v)

    for ((k, v) <- pathVariablesTypes)
      sum.bindPathVariableType(k, v._1, v._2, v._3)

    for ((k, v) <- that.pathVariablesTypes)
      sum.bindPathVariableType(k, v._1, v._2, v._3)

    sum
  }
}

object BindingsSchema {
  def apply() = new BindingsSchema(None, false)
  
  def apply(parent: BindingsSchema, updatesParent: Boolean) = new BindingsSchema(Some(parent), updatesParent)
}