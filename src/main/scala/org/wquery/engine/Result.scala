package org.wquery.engine

import org.wquery.WQueryException

sealed abstract class Result

case class Answer(dataSet: DataSet) extends Result
case class Error(exception: WQueryException) extends Result
