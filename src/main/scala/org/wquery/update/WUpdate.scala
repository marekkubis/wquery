package org.wquery.update

import org.wquery.model.WordNet
import org.wquery.lang.WLanguage
import org.wquery.update.parsers.WUpdateParsers

class WUpdate(override val wordNet: WordNet) extends WLanguage(wordNet, new Object with WUpdateParsers)
