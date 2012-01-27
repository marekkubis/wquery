package org.wquery.engine.planner

import org.wquery.engine.operations._
import org.wquery.model.{Backward, Forward}
import org.wquery.engine.{PathVariable, StepVariable, VariableTemplate}
import scalaz._
import Scalaz._

sealed abstract class Link(val variables: VariableTemplate) {
  def leftFringe: AlgebraOp
  def rightFringe: List[(AlgebraOp, Option[Condition])]
  def forward(op: AlgebraOp): AlgebraOp
  def backward(op: AlgebraOp): AlgebraOp
}

class RootLink(generator: AlgebraOp, override val variables: VariableTemplate) extends Link(variables) {
  def leftFringe = ConstantOp.empty

  def rightFringe = List((generator, none[Condition]))

  def forward(op: AlgebraOp) = generator

  def backward(op: AlgebraOp) = {
    val template = VariableTemplate(List(StepVariable("__a"), PathVariable("_")))
    val condition = BinaryCondition("in", StepVariableRefOp(StepVariable("__a"), generator.leftType(0)), generator)

    SelectOp(BindOp(op, template), condition)
  }

  override def toString = "r(" + generator + ")"
}

class PatternLink(val pos: Int, pattern: RelationalPattern, override val variables: VariableTemplate, rightGenerators: List[(AlgebraOp, Condition)]) extends Link(variables) {
  def leftFringe = FringeOp(pattern, Left)

  def rightFringe = (FringeOp(pattern, Right), none[Condition])::(rightGenerators.map{ case (a, b) => (a, b.some) })

  def forward(op: AlgebraOp) = ExtendOp(op, pos, pattern, Forward, variables)

  def backward(op: AlgebraOp) = ExtendOp(op, pos, pattern, Backward, variables)

  override def toString = "p(" + List(pos, pattern, variables).mkString(",") + ")"
}