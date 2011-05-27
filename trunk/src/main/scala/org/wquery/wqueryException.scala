package org.wquery

sealed abstract class WQueryException(message: String) extends RuntimeException(message)

class WQueryParsingException(message: String) extends WQueryException(message)

class WQueryParsingFailureException(message: String) extends WQueryParsingException(message)

class WQueryParsingErrorException(message: String) extends WQueryParsingException(message)

class WQueryEvaluationException(message: String) extends WQueryException(message)

class WQueryModelException(message: String) extends WQueryException(message)

class WQueryLoadingException(message: String) extends WQueryException(message)