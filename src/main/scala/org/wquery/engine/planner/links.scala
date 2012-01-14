package org.wquery.engine.planner

import org.wquery.engine.operations._
import org.wquery.model.{Backward, Forward}
import org.wquery.engine.{PathVariable, StepVariable, VariableTemplate}

sealed abstract class Link(val variables: VariableTemplate) {
  def leftFringe: AlgebraOp
  def rightFringe: AlgebraOp
  def forward(op: AlgebraOp): AlgebraOp
  def backward(op: AlgebraOp): AlgebraOp
}

class RootLink(generator: AlgebraOp, override val variables: VariableTemplate) extends Link(variables) {
  def leftFringe = ConstantOp.empty

  def rightFringe = generator

  def forward(op: AlgebraOp) = generator

  def backward(op: AlgebraOp) = {
    val template = VariableTemplate(List(StepVariable("__a"), PathVariable("_")))
    val condition = BinaryCondition("in", StepVariableRefOp(StepVariable("__a"), generator.leftType(0)), generator)

    SelectOp(BindOp(op, template), condition)
  }

  override def toString = "r(" + generator + ")"
}

// class PatternLink(val leftGenerator: Option[AlgebraOp], val pos: Int, val pattern: RelationalPattern, val variables: VariableTemplate, val rightGenerator: Option[AlgebraOp]) extends Link {
class PatternLink(pos: Int, pattern: RelationalPattern, override val variables: VariableTemplate) extends Link(variables) {
  def leftFringe = FringeOp(pattern, Left)

  def rightFringe = FringeOp(pattern, Right)

  def forward(op: AlgebraOp) = ExtendOp(op, pos, pattern, Forward, variables)

  def backward(op: AlgebraOp) = ExtendOp(op, pos, pattern, Backward, variables)

  override def toString = "p(" + List(pos, pattern, variables).mkString(",") + ")"
}