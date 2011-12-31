package org.wquery.engine

import scalaz._
import Scalaz._
import org.wquery.model._
import collection.mutable.ListBuffer
import org.wquery.WQueryEvaluationException

sealed abstract class RelationalPattern {
  def extend(wordNet: WordNetStore, bindings: Bindings, extensionSet: ExtensionSet, from: Int, direction: Direction): ExtendedExtensionSet

  def minSize: Int

  def maxSize: Option[Int]

  def sourceType: Set[DataType]

  def leftType(pos: Int): Set[DataType]

  def rightType(pos: Int): Set[DataType]
}

case class RelationUnionPattern(patterns: List[RelationalPattern]) extends RelationalPattern {
  def extend(wordNet: WordNetStore, bindings: Bindings, extensionSet: ExtensionSet, from: Int, direction: Direction) = {
    val buffer = new ExtensionSetBuffer(extensionSet)

    patterns.foreach(expr => buffer.append(expr.extend(wordNet, bindings, extensionSet, from, direction)))
    buffer.toExtensionSet
  }

  def minSize = patterns.map(_.minSize).min

  def maxSize = if (patterns.exists(!_.maxSize.isDefined)) none else patterns.map(_.maxSize).max

  def sourceType = patterns.flatMap(_.sourceType).toSet

  def leftType(pos: Int) = patterns.flatMap(_.leftType(pos)).toSet

  def rightType(pos: Int) = patterns.flatMap(_.rightType(pos)).toSet
}

case class RelationCompositionPattern(patterns: List[RelationalPattern]) extends RelationalPattern {
  def extend(wordNet: WordNetStore, bindings: Bindings, extensionSet: ExtensionSet, from: Int, direction: Direction) = {
    val headSet = patterns.head.extend(wordNet, bindings, extensionSet, from, direction)

    patterns.tail.foldLeft(headSet)((dataSet, expr) => expr.extend(wordNet, bindings, dataSet, 0, direction))
  }

  def minSize = patterns.map(_.minSize).sum

  def maxSize = {
    if (patterns.exists(!_.maxSize.isDefined))
      none
    else
      some(patterns.map(_.maxSize).collect{ case Some(num) => num }.sum)
  }

  def sourceType = patterns.head.sourceType

  def leftType(pos: Int) = leftType(patterns, pos)

  private def leftType(patterns: List[RelationalPattern], pos: Int): Set[DataType] = {
    patterns.headOption.map { headPattern =>
      if (pos < headPattern.minSize)
        headPattern.leftType(pos)
      else if (headPattern.maxSize.map(pos < _).getOrElse(true)) { // pos < maxSize or maxSize undefined
        headPattern.leftType(pos) union leftType(patterns.tail, pos - headPattern.minSize)
      } else { // else pos < maxSize and defined
        leftType(patterns.tail, pos - headPattern.maxSize.get)
      }
    }.getOrElse(Set.empty)
  }

  def rightType(pos: Int) = rightType(patterns.reverse, pos)

  private def rightType(patterns: List[RelationalPattern], pos: Int): Set[DataType] = {
    patterns.headOption.map { headPattern =>
      if (pos < headPattern.minSize)
        headPattern.rightType(pos)
      else if (headPattern.maxSize.map(pos < _).getOrElse(true)) { // pos < maxSize or maxSize undefined
        headPattern.rightType(pos) union rightType(patterns.tail, pos - headPattern.minSize)
      } else { // else pos < maxSize and defined
        rightType(patterns.tail, pos - headPattern.maxSize.get)
      }
    }.getOrElse(Set.empty)
  }
}

case class QuantifiedRelationPattern(pattern: RelationalPattern, quantifier: Quantifier) extends RelationalPattern {
  def extend(wordNet: WordNetStore, bindings: Bindings, extensionSet: ExtensionSet, from: Int, direction: Direction) = {
    val lowerExtensionSet = (1 to quantifier.lowerBound).foldLeft(extensionSet)((x, _) => pattern.extend(wordNet, bindings, x, from, direction))

    if (some(quantifier.lowerBound) != quantifier.upperBound)
      computeClosure(wordNet, bindings, lowerExtensionSet, from, direction, quantifier.upperBound.map(_ - quantifier.lowerBound))
    else
      lowerExtensionSet.asInstanceOf[ExtendedExtensionSet] // valid quantifiers invoke extend at least once
  }

  private def computeClosure(wordNet: WordNetStore, bindings: Bindings, extensionSet: ExtensionSet, from: Int, direction: Direction, limit: Option[Int]) = {
    val builder = new ExtensionSetBuilder(extensionSet)

    for (i <- 0 until extensionSet.size) {
      val source = extensionSet.right(i, from)
      builder.extend(i, Nil)

      for (cnode <- closeTuple(wordNet, bindings, direction, List(source), Set(source), limit))
        builder.extend(i, cnode)
    }

    builder.build
  }

  private def closeTuple(wordNet: WordNetStore, bindings: Bindings, direction: Direction, source: List[Any], forbidden: Set[Any], limit: Option[Int]): Seq[List[Any]] = {
    if (limit.getOrElse(1) > 0) {
      val transformed = pattern.extend(wordNet, bindings, new DataExtensionSet(DataSet.fromList(source)), 0, direction)
      val filtered = transformed.extensions.filter(x => !forbidden.contains(x.last)).map(_.tail)

      if (filtered.isEmpty) {
        filtered
      } else {
        val result = new ListBuffer[List[Any]]
        val newForbidden = forbidden ++ filtered.map(_.last)
        val newLimit = limit.map(_ - 1)

        for (extension <- filtered) {
          val newSource = source ++ extension

          result.append(newSource.tail)
          result.appendAll(closeTuple(wordNet, bindings, direction, newSource, newForbidden, newLimit))
        }

        result
      }
    } else {
      Nil
    }
  }

  def minSize = pattern.minSize * quantifier.lowerBound

  def maxSize = (pattern.maxSize <|*|> quantifier.upperBound).map{ case (a, b) => a * b }

  def sourceType = pattern.sourceType

  def leftType(pos: Int) = if (maxSize.map(pos < _).getOrElse(true)) {
    (for (i <- pattern.minSize to pattern.maxSize.getOrElse(pos + 1))
    yield pattern.leftType(if (i > 0) pos % i else 0)).flatten.toSet
  } else {
    Set[DataType]()
  }

  def rightType(pos: Int) = if (maxSize.map(pos < _).getOrElse(true)) {
    (for (i <- pattern.minSize to pattern.maxSize.getOrElse(pos + 1))
    yield pattern.rightType(if (i > 0) pos % i else 0)).flatten.toSet
  } else {
    Set[DataType]()
  }
}

case class ArcPattern(relation: Option[Relation], source: ArcPatternArgument, destinations: List[ArcPatternArgument]) extends RelationalPattern {
  def extend(wordNet: WordNetStore, bindings: Bindings, extensionSet: ExtensionSet, from: Int, direction: Direction) = {
    relation.map(wordNet.extend(extensionSet, _, from, direction, source.name, destinations.map(_.name)))
      .getOrElse(wordNet.extend(extensionSet, from, direction, (source.name, source.nodeType), if (destinations == List(ArcPatternArgument("_", None))) Nil else destinations.map(dest => (dest.name, dest.nodeType))))
  }

  def minSize = 2*destinations.size

  def maxSize = some(2*destinations.size)

  def sourceType = demandArgumentType(source)

  def leftType(pos: Int) = {
    if (pos < minSize)
      if (pos % 2 == 0) {
        Set(ArcType)
      } else {
        demandArgumentType(destinations((pos - 1)/2))
      }
    else
      Set.empty
  }

  def rightType(pos: Int) = {
    if (pos < destinations.size)
      if (pos % 2 == 1) {
        Set(ArcType)
      } else {
        demandArgumentType(destinations(destinations.size - 1 - pos/2))
      }
    else
      Set.empty
  }

  override def toString = (source::relation.map(_.name).getOrElse("_")::destinations).mkString("^")

  private def demandArgumentType(argument: ArcPatternArgument) = {
    relation.map(rel => Set(rel.demandArgument(argument.name).nodeType))
      .getOrElse(argument.nodeType.map(Set(_)).getOrElse(NodeType.all)).asInstanceOf[Set[DataType]]
  }
}

case class VariableRelationalPattern(variable: StepVariable) extends RelationalPattern {
  def extend(wordNet: WordNetStore, bindings: Bindings, extensionSet: ExtensionSet, from: Int, direction: Direction) = {
    bindings.lookupStepVariable(variable.name).map {
      case Arc(relation, source, destination) =>
        wordNet.extend(extensionSet, relation, from, direction, source, List(destination))
      case _ =>
        throw new WQueryEvaluationException("Cannot extend a path using a non-arc value of variable " + variable)
    }.getOrElse(throw new WQueryEvaluationException("Variable " + variable + " is not bound"))
  }

  def minSize = 2

  def maxSize = some(2)

  def sourceType = DataType.all

  def leftType(pos: Int) = pos match {
    case 0 =>
      Set(ArcType)
    case 1 =>
      DataType.all
    case _ =>
      Set.empty
  }

  def rightType(pos: Int) = pos match {
    case 0 =>
      DataType.all
    case 1 =>
      Set(ArcType)
    case _ =>
      Set.empty
  }

  override def toString = variable.toString
}
