package org.wquery.query

import org.wquery.lang.WLanguageMain
import org.wquery.model.WordNet

object WQueryMain extends WLanguageMain {
  override def languageName = "WQuery"

  override def language(wordNet: WordNet) = new WQuery(wordNet)
}
