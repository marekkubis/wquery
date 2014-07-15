package org.wquery.path

import org.wquery.lang.WLanguageMain
import org.wquery.model.WordNet

object WPathMain extends WLanguageMain {
  override def languageName = "WPath"

  override def language(wordNet: WordNet) = new WPath(wordNet)
}
