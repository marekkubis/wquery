// scalastyle:off multiple.string.literals

package org.wquery

import org.wquery.lang.Variable
import org.wquery.model.Relation

sealed abstract class WQueryException(message: String) extends RuntimeException(message)

class WQueryParsingException(message: String) extends WQueryException(message)

class WQueryParsingFailureException(message: String) extends WQueryParsingException(message)

class WQueryParsingErrorException(message: String) extends WQueryParsingException(message)

class WQueryStaticCheckException(message: String) extends WQueryException(message)

class WQueryStepVariableCannotBeBoundException(variableName: String)
  extends WQueryStaticCheckException("Variable $" + variableName + " cannot be bound")

class FoundReferenceToUnknownVariableWhileCheckingException(variable: Variable)
  extends WQueryStaticCheckException("A reference to unknown variable " + variable + " found")

class WQueryEvaluationException(message: String) extends WQueryException(message)

class WQueryInvalidValueSpecifiedForRelationPropertyException(property: String)
  extends WQueryEvaluationException("Invalid value specified for relation property '" + property + "'")

class FoundReferenceToUnknownVariableWhileEvaluatingException(variable: Variable)
  extends WQueryEvaluationException("A reference to unknown variable " + variable + " found")

class WQueryModelException(message: String) extends WQueryException(message)

class WQueryUpdateBreaksRelationPropertyException(val property: String, val relation: Relation, val argument: String = "")
  extends WQueryModelException("Update breaks property '" + property + "' of relation '" +
    relation.name + "'" + (if (!argument.isEmpty) " on argument '" + argument + "'" else ""))

class WQueryLoadingException(message: String) extends WQueryException(message)

class WQueryLoaderNotFoundException(name: String) extends WQueryLoadingException("Loader '" + name + "' not found")

class WQueryCommandLineException(val message: String) extends WQueryException(message)

class WQueryEmitterException(message: String) extends WQueryException(message)

class WQueryEmitterNotFoundException(name: String) extends WQueryEmitterException("Emitter '" + name + "' not found")
