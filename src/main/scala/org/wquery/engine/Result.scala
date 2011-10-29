package org.wquery.engine
import org.wquery.WQueryException
import org.wquery.model.{WordNet, DataSet}

sealed abstract class Result

case class Answer(wordNet: WordNet, dataSet: DataSet) extends Result
case class Error(exception: WQueryException) extends Result
