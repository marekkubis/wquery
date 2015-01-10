package org.wquery.lang

import java.util

import jline.console.completer.Completer
import org.wquery.model.{NodeType, WordNet}

class WLanguageCompleter(wordNet: WordNet) extends Completer {
  override def complete(buffer: String, cursor: Int, candidates: util.List[CharSequence]) = {
    var offset = 0

    if (buffer != null && buffer.nonEmpty) {
      val lastRelationIndex = Math.max(buffer.lastIndexOf('.'), buffer.lastIndexOf('^'))
      val lastTypeIndex = buffer.lastIndexOf('&')

      if (lastRelationIndex > lastTypeIndex) {
        val prefix = buffer.substring(lastRelationIndex + 1)

        if (prefix.isEmpty || prefix.head.isLetter && prefix.forall(c => c.isLetterOrDigit || c == '_')) {
          for (name <- wordNet.relations.filter(_.arguments.size == 2).map(_.name) if name.startsWith(prefix))
            candidates.add(name)
        }

        offset = prefix.size
      } else if (lastRelationIndex < lastTypeIndex) {
        val prefix = buffer.substring(lastTypeIndex + 1)

        if (prefix.forall(_.isLetter)) {
          for (name <- NodeType.nameTypes.keys if name.startsWith(prefix))
            candidates.add(name)
        }

        offset = prefix.size
      }
    }

    if (candidates.isEmpty) -1 else buffer.length() - offset
  }
}
