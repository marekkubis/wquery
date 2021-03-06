package org.wquery.update

import org.wquery.lang.Variable
import org.wquery.lang.operations._
import org.wquery.path.{TupleVariable, StepVariable}
import org.wquery.query.SetVariable
import org.wquery.model.{WordNet, Relation, DataType}
import org.wquery.WQueryEvaluationException

case class RelationSpecification(arguments: List[RelationSpecificationArgument]) {
  def resolve(wordNet: WordNet#Schema, bindings: Bindings, leftSize: Int): (Relation, List[String], List[String]) = {
    val identifiers = arguments.map(_.resolveIdentifier(bindings))

    if (identifiers.size == 1) {
      val relation = wordNet.demandRelation(identifiers.head, Map(), true)
      val (leftArgs, rightArgs) = relation.argumentNames.splitAt(leftSize)

      (relation, leftArgs, rightArgs)
    } else {
      val (leftArgs, relationName::rightArgs) = identifiers.splitAt(leftSize)

      (wordNet.demandRelation(relationName, (leftArgs ++ rightArgs).map(x => (x, DataType.all)).toMap), leftArgs, rightArgs)
    }
  }
}

sealed abstract class RelationSpecificationArgument {
  def resolveIdentifier(bindings: Bindings): String
}

case class VariableRelationSpecificationArgument(variable: Variable) extends RelationSpecificationArgument{
  def resolveIdentifier(bindings: Bindings) = {
    val value = variable match {
      case StepVariable(name) =>
        bindings.demandStepVariable(name)
      case TupleVariable(name) =>
        val tuple = bindings.demandTupleVariable(name)

        if (tuple.size == 1)
          tuple.head
        else
          throw new WQueryEvaluationException("Tuple variable " + name + " in relation specification must contain exactly one value")
      case SetVariable(name) =>
        val dataSet = bindings.demandSetVariable(name)

        if (dataSet.pathCount == 1 && dataSet.paths.head.size == 1)
          dataSet.paths.head.head
        else
          throw new WQueryEvaluationException("Data set variable " + name + " reference in relation specification must contain exactly one value")
    }

    value match {
      case str: String =>
        str
      case _ =>
        throw new WQueryEvaluationException("Variable " + variable + " does not contain a character string")
    }
  }
}

case class ConstantRelationSpecificationArgument(identifier: String) extends RelationSpecificationArgument{
  def resolveIdentifier(bindings: Bindings) = identifier
}
