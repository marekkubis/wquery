package org.wquery.path

import org.wquery.model.WordNet
import org.wquery.lang.WLanguage
import org.wquery.path.parsers.WPathParsers

class WPath(override val wordNet: WordNet) extends WLanguage(wordNet, new Object with WPathParsers)
