package org.wquery.engine.operations

import scalaz._
import Scalaz._
import org.wquery.model._
import org.wquery.WQueryInvalidValueSpecifiedForRelationPropertyException
import scala.collection.mutable.{Map => MMap}
import org.wquery.engine.Variable

sealed abstract class UpdateOp extends AlgebraOp {
  def update(wordNet: WordNet, bindings: Bindings)

  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    update(wordNet, bindings)
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


sealed abstract class TupleUpdateOp(val leftOp: AlgebraOp, val relationalPattern: ArcRelationalPattern, val rightOp: AlgebraOp) extends UpdateOp {
  val referencedVariables = leftOp.referencedVariables ++ rightOp.referencedVariables

  def update(wordNet: WordNet, bindings: Bindings) {
    val leftSet = leftOp.evaluate(wordNet, bindings)
    val rightSet = rightOp.evaluate(wordNet, bindings)
    val destinationNames = relationalPattern.pattern.destinations.map(_.name)

    for (sources <- leftSet.paths; destinations <- rightSet.paths) {
      updateWith(wordNet, bindings, relationalPattern.pattern.relation.get, ((relationalPattern.pattern.source.name, sources.last) :: destinationNames.zip(destinations)).toMap)
    }
  }

  def updateWith(wordNet: WordNet, bindings: Bindings, relation: Relation, tuple: Map[String, Any])
}

case class AddTuplesOp(override val leftOp: AlgebraOp, override val relationalPattern: ArcRelationalPattern, override val rightOp: AlgebraOp) extends TupleUpdateOp(leftOp, relationalPattern, rightOp) {
  def updateWith(wordNet: WordNet, bindings: Bindings, relation: Relation, tuple: Map[String, Any]) {
    wordNet.store.addLink(relation, tuple)
  }
}

case class RemoveTuplesOp(override val leftOp: AlgebraOp, override val relationalPattern: ArcRelationalPattern, override val rightOp: AlgebraOp) extends TupleUpdateOp(leftOp, relationalPattern, rightOp) {
  def updateWith(wordNet: WordNet, bindings: Bindings, relation: Relation, tuple: Map[String, Any]) {
    wordNet.store.removeMatchingLinks(relation, tuple)
  }
}

case class SetTuplesOp(leftOp:AlgebraOp, relationalPattern: ArcRelationalPattern, rightOp:AlgebraOp) extends UpdateOp {
  def update(wordNet: WordNet, bindings: Bindings) {
    val leftSet = leftOp.evaluate(wordNet, bindings)
    val rightSet = rightOp.evaluate(wordNet, bindings)

    if (!rightSet.isEmpty) {
      val destinationNames = relationalPattern.pattern.destinations.map(_.name)

      for (tuple <- leftSet.paths) {
        wordNet.store.setLinks(relationalPattern.pattern.relation.get, relationalPattern.pattern.source.name, tuple.last,
          (for (destinations <- rightSet.paths) yield ((relationalPattern.pattern.source.name, tuple.last)::destinationNames.zip(destinations)).toMap))
      }
    } else {
      for(tuple <- leftSet.paths)
        wordNet.store.removeMatchingLinks(relationalPattern.pattern.relation.get, Map((relationalPattern.pattern.source.name, tuple.last)))
    }
  }

  val referencedVariables = leftOp.referencedVariables ++ rightOp.referencedVariables
}

sealed abstract class WordNetUpdateWithAssignmentsOp[A](val valuesOp: AlgebraOp, val patterns: List[PropertyAssignmentPattern]) extends UpdateOp {
  val referencedVariables = patterns.foldLeft(valuesOp.referencedVariables)((l, r) => l ++ r.referencedVariables)

  def update(wordNet: WordNet, bindings: Bindings) {
    updateWithAssignments(wordNet, bindings, patterns.map(_.evaluatePattern(wordNet, bindings)))
  }

  def updateWithAssignments(wordNet: WordNet, bindings: Bindings, assignments: List[PropertyAssignment])
}

sealed abstract class WordNetUpdateWithValuesOp[A](override val valuesOp: AlgebraOp, override val patterns: List[PropertyAssignmentPattern]) extends WordNetUpdateWithAssignmentsOp(valuesOp, patterns) {
  def updateWithAssignments(wordNet: WordNet, bindings: Bindings, assignments: List[PropertyAssignment]) {
    valuesOp.evaluate(wordNet, bindings).paths.foreach(tuple => updateWithValue(wordNet, bindings, assignments, tuple.last.asInstanceOf[A]))
  }

  def updateWithValue(wordNet: WordNet, bindings: Bindings, assignments: List[PropertyAssignment], value: A)
}

case class PropertyAssignmentPattern(relationalPattern: ArcRelationalPattern, op: String, valuesOp: AlgebraOp) {
  def evaluatePattern(wordNet: WordNet, bindings: Bindings) = {
    PropertyAssignment(relationalPattern.pattern, op, valuesOp.evaluate(wordNet, bindings))
  }

  val referencedVariables = valuesOp.referencedVariables
}

case class AddSensesOp(override val valuesOp: AlgebraOp, override val patterns: List[PropertyAssignmentPattern]) extends WordNetUpdateWithValuesOp[Sense](valuesOp, patterns) {
  def updateWithValue(wordNet: WordNet, bindings: Bindings, assignments: List[PropertyAssignment], sense: Sense) {
    wordNet.store.addSense(sense, assignments)
  }
}

case class AddSynsetsOp(override val valuesOp: AlgebraOp, override val patterns: List[PropertyAssignmentPattern]) extends WordNetUpdateWithAssignmentsOp(valuesOp, patterns) {
  def updateWithAssignments(wordNet: WordNet, bindings: Bindings, assignments: List[PropertyAssignment]) = {
    for (tuple <- valuesOp.evaluate(wordNet, bindings).paths) {
      tuple.last match {
        case newSynset: NewSynset =>
          wordNet.store.addSynset(none, newSynset.senses, assignments)
        case _ =>
          // do nothing
      }
    }
  }
}

case class AddWordsOp(override val valuesOp: AlgebraOp, override val patterns: List[PropertyAssignmentPattern]) extends WordNetUpdateWithValuesOp[String](valuesOp, patterns) {
  def updateWithValue(wordNet: WordNet, bindings: Bindings, assignments: List[PropertyAssignment], word: String) {
    wordNet.store.addWord(word, assignments)
  }
}

case class AddPartOfSpeechSymbolsOp(override val valuesOp: AlgebraOp, override val patterns: List[PropertyAssignmentPattern]) extends WordNetUpdateWithValuesOp[String](valuesOp, patterns) {
  def updateWithValue(wordNet: WordNet, bindings: Bindings, assignments: List[PropertyAssignment], pos: String) {
    wordNet.store.addPartOfSpeechSymbol(pos, assignments)
  }
}

case class AddRelationsOp(override val valuesOp: AlgebraOp, override val patterns: List[PropertyAssignmentPattern]) extends WordNetUpdateWithAssignmentsOp(valuesOp, patterns) {
  def updateWithAssignments(wordNet: WordNet, bindings: Bindings, assignments: List[PropertyAssignment]) = {
    val valueSet = valuesOp.evaluate(wordNet, bindings)
    valueSet.paths.map(_.last.asInstanceOf[Arc].relation).distinct.foreach(wordNet.store.addRelation(_))
  }
}

case class RemoveSensesOp(override val valuesOp: AlgebraOp, override val patterns: List[PropertyAssignmentPattern]) extends WordNetUpdateWithValuesOp[Sense](valuesOp, patterns) {
  def updateWithValue(wordNet: WordNet, bindings: Bindings, assignments: List[PropertyAssignment], sense: Sense) {
    wordNet.store.removeSense(sense)
  }
}

case class RemoveSynsetsOp(override val valuesOp: AlgebraOp, override val patterns: List[PropertyAssignmentPattern]) extends WordNetUpdateWithValuesOp[Synset](valuesOp, patterns) {
  def updateWithValue(wordNet: WordNet, bindings: Bindings, assignments: List[PropertyAssignment], synset: Synset) {
    wordNet.store.removeSynset(synset)
  }
}

case class RemoveWordsOp(override val valuesOp: AlgebraOp, override val patterns: List[PropertyAssignmentPattern]) extends WordNetUpdateWithValuesOp[String](valuesOp, patterns) {
  def updateWithValue(wordNet: WordNet, bindings: Bindings, assignments: List[PropertyAssignment], word: String) {
    wordNet.store.removeWord(word)
  }
}

case class RemovePartOfSpeechSymbolsOp(override val valuesOp: AlgebraOp, override val patterns: List[PropertyAssignmentPattern]) extends WordNetUpdateWithValuesOp[String](valuesOp, patterns) {
  def updateWithValue(wordNet: WordNet, bindings: Bindings, assignments: List[PropertyAssignment], pos: String) {
    wordNet.store.removePartOfSpeechSymbol(pos)
  }
}

case class RemoveRelationsOp(override val valuesOp: AlgebraOp, override val patterns: List[PropertyAssignmentPattern]) extends WordNetUpdateWithAssignmentsOp(valuesOp, patterns) {
  def updateWithAssignments(wordNet: WordNet, bindings: Bindings, assignments: List[PropertyAssignment]) = {
    val valueSet = valuesOp.evaluate(wordNet, bindings)
    valueSet.paths.map(_.last.asInstanceOf[Arc].relation).distinct.foreach(wordNet.store.removeRelation(_))
  }
}

case class SetSensesOp(override val valuesOp: AlgebraOp, override val patterns: List[PropertyAssignmentPattern]) extends WordNetUpdateWithAssignmentsOp(valuesOp, patterns) {
  def updateWithAssignments(wordNet: WordNet, bindings: Bindings, assignments: List[PropertyAssignment]) = {
    wordNet.store.setSenses(valuesOp.evaluate(wordNet, bindings).paths.map(_.last.asInstanceOf[Sense]), assignments)
  }
}

case class SetSynsetsOp(override val valuesOp: AlgebraOp, override val patterns: List[PropertyAssignmentPattern]) extends WordNetUpdateWithAssignmentsOp(valuesOp, patterns) {
  def updateWithAssignments(wordNet: WordNet, bindings: Bindings, assignments: List[PropertyAssignment]) = {
    wordNet.store.setSynsets(valuesOp.evaluate(wordNet, bindings).paths.map(_.last.asInstanceOf[Synset]), assignments)
  }
}

case class SetWordsOp(override val valuesOp: AlgebraOp, override val patterns: List[PropertyAssignmentPattern]) extends WordNetUpdateWithAssignmentsOp(valuesOp, patterns) {
  def updateWithAssignments(wordNet: WordNet, bindings: Bindings, assignments: List[PropertyAssignment]) = {
    wordNet.store.setWords(valuesOp.evaluate(wordNet, bindings).paths.map(_.last.asInstanceOf[String]), assignments)
  }
}

case class SetPartOfSpeechSymbolsOp(override val valuesOp: AlgebraOp, override val patterns: List[PropertyAssignmentPattern]) extends WordNetUpdateWithAssignmentsOp(valuesOp, patterns) {
  def updateWithAssignments(wordNet: WordNet, bindings: Bindings, assignments: List[PropertyAssignment]) = {
    wordNet.store.setPartOfSpeechSymbols(valuesOp.evaluate(wordNet, bindings).paths.map(_.last.asInstanceOf[String]), assignments)
  }
}

case class SetRelationsOp(override val valuesOp: AlgebraOp, override val patterns: List[PropertyAssignmentPattern]) extends WordNetUpdateWithAssignmentsOp(valuesOp, patterns) {
  def updateWithAssignments(wordNet: WordNet, bindings: Bindings, assignments: List[PropertyAssignment]) = {
    val valueSet = valuesOp.evaluate(wordNet, bindings)
    val newRelations = valueSet.paths.map(_.last.asInstanceOf[Arc].relation).distinct

    wordNet.store.setRelations(newRelations, assignments)
  }
}

case class NewSynsetOp(sensesOp: AlgebraOp) extends AlgebraOp {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val senses = sensesOp.evaluate(wordNet, bindings).paths.map(_.last.asInstanceOf[Sense])

    if (!senses.isEmpty) {
      val synset = wordNet.store.getSynset(senses.head).map { synset =>
        if (wordNet.store.getSenses(synset) == senses)
          synset
        else
          new NewSynset(senses)
      }.getOrElse(new NewSynset(senses))

      DataSet.fromValue(synset)
    } else {
      DataSet.empty
    }
  }

  def leftType(pos: Int) = if (pos == 0) Set(SynsetType) else Set.empty

  def rightType(pos: Int) = if (pos == 0) Set(SynsetType) else Set.empty

  val minTupleSize = 1

  val maxTupleSize = some(1)

  def bindingsPattern = BindingsPattern()

  val referencedVariables = sensesOp.referencedVariables

  def maxCount(wordNet: WordNetSchema) = some(1)

  def cost(wordNet: WordNetSchema) = maxSize(wordNet)
}

class NewSynset(val senses: List[Sense]) extends Synset("synset#" + senses.head.toString)

case class CreateRelationFromPatternOp(name: String, pattern: RelationalPattern, sourceType: NodeType, destinationType: NodeType) extends UpdateOp {
  def update(wordNet: WordNet, bindings: Bindings) {
    wordNet.store.schema.getRelation(name, Map((Relation.Source, pattern.sourceType))).map(wordNet.store.removeRelation(_))

    val relation = Relation.binary(name, sourceType, destinationType)

    wordNet.store.addRelation(relation)
    wordNet.store.addRelationPattern(relation, pattern)
  }

  val referencedVariables = ∅[Set[Variable]]
}

case class RelationUpdateOp(arcsOp: AlgebraOp, op: String, property: String, action: Boolean, valuesOp: AlgebraOp) extends UpdateOp {
  val referencedVariables = arcsOp.referencedVariables ++ valuesOp.referencedVariables

  def update(wordNet: WordNet, bindings: Bindings) = {
    val arcs = arcsOp.evaluate(wordNet, bindings).paths.map(_.last.asInstanceOf[Arc])
    val value = valuesOp.evaluate(wordNet, bindings).paths.map(_.last).head

    if (action) {
      if (Relation.PropertyActions.contains(value)) {
        property match {
          case Relation.Transitivity =>
            updateRelationPropertyAction(wordNet.store.transitivesActions, arcs, value)
          case Relation.Symmetry =>
            updateRelationPropertyAction(wordNet.store.symmetryActions, arcs, value)
          case Relation.Functional =>
            updateRelationArgumentPropertyAction(wordNet.store.functionalForActions, arcs, value)
        }
      } else {
        throw new WQueryInvalidValueSpecifiedForRelationPropertyException(property)
      }
    } else {
      property match {
        case Relation.Transitivity =>
          updateTransitivityProperty(wordNet.store, arcs, value)
        case Relation.Symmetry =>
          updateSymmetryProperty(wordNet.store, arcs, value)
        case Relation.RequiredBy =>
          updateRelationArgumentProperty(wordNet.store.requiredBys, arcs, value)
        case Relation.Functional =>
          updateRelationArgumentProperty(wordNet.store.functionalFor, arcs, value)
      }
    }
  }

  private def updateTransitivityProperty(store: WordNetStore, arcs: List[Arc], value: Any) {
    val logicValue = value.asInstanceOf[Boolean]

    if (op == "+=" || op == ":=") {
      for (arc <- arcs)
        store.transitives(arc.relation) = logicValue
    } else {
      for (arc <- arcs) {
        if (store.transitives(arc.relation) == value)
          store.transitives(arc.relation) = !logicValue
      }
    }
  }

  private def updateSymmetryProperty(store: WordNetStore, arcs: List[Arc], value: Any) {
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
        store.symmetry(arc.relation) = symmetry
    } else {
      for (arc <- arcs) {
        if (store.symmetry(arc.relation) == symmetry)
          store.symmetry(arc.relation) = NonSymmetric
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
// Merge & Split
//

case class MergeOp(override val valuesOp: AlgebraOp, override val patterns: List[PropertyAssignmentPattern]) extends WordNetUpdateWithAssignmentsOp(valuesOp, patterns) {
  def updateWithAssignments(wordNet: WordNet, bindings: Bindings, assignments: List[PropertyAssignment]) = {
    val (synsets, senses) = valuesOp.evaluate(wordNet, bindings).paths.map(_.last).partition(_.isInstanceOf[Synset]).asInstanceOf[(List[Synset], List[Sense])]
    wordNet.store.merge(synsets, senses, assignments)
  }
}

case class SplitOp(override val valuesOp: AlgebraOp, override val patterns: List[PropertyAssignmentPattern]) extends WordNetUpdateWithAssignmentsOp(valuesOp, patterns) {
  def updateWithAssignments(wordNet: WordNet, bindings: Bindings, assignments: List[PropertyAssignment]) = {
    val synsets = valuesOp.evaluate(wordNet, bindings).paths.map(_.last).asInstanceOf[List[Synset]]
    wordNet.store.split(synsets, assignments)
  }
}
