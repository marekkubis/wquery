package org.wquery.query

import org.wquery.model.WordNet
import org.wquery.lang.WLanguage
import org.wquery.query.parsers.WQueryParsers

class WQuery(override val wordNet: WordNet) extends WLanguage(wordNet, new Object with WQueryParsers)
