package org.wquery.lang

import org.wquery.model._

trait WTupleParsers extends WTokenParsers {
  val DefaultSeparator = "\t"

  def parse(wordNet: WordNet, value: String, sep: String = DefaultSeparator) = {
    value.split(sep).map { elem =>
      parseAll(tokenLit, elem) match {
        case Success((SynsetType, (word: String, num: Int, pos: String)), _) =>
          wordNet.getSense(word, num, pos).map(wordNet.getSynset).flatten
        case Success((SenseType, (word: String, num: Int, pos: String)), _) =>
          wordNet.getSense(word, num, pos)
        case Success((StringType, false, value: String), _) =>
          Some(value)
        case Success((StringType, true, value: String), _) =>
          wordNet.getWord(value)
        case Success((FloatType, num), _) =>
          Some(num)
        case Success((IntegerType, num), _) =>
          Some(num)
        case _ =>
          None
      }
    }.toList.flatten
  }

}
