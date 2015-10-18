package org.wquery.lang

import java.util

import jline.console.completer.Completer

class WLanguageCompleter(lang: WLanguage) extends Completer {
  def isIdentifier(value: String) = {
    value.isEmpty || value.head.isLetter && value.forall(c => c.isLetterOrDigit || c == '_')
  }

  override def complete(buffer: String, cursor: Int, candidates: util.List[CharSequence]) = {
    var offset = 0

    if (buffer != null && buffer.nonEmpty) {
      val lastRelationIndex = List(buffer.lastIndexOf('.'), buffer.lastIndexOf('^'), buffer.lastIndexOf('!'), buffer.lastIndexOf('\\')).max
      val lastVariableIndex = buffer.lastIndexOf('%')

      if (lastRelationIndex > lastVariableIndex) {
        val prefix = buffer.substring(lastRelationIndex + 1)

        if (isIdentifier(prefix)) {
          for (name <- lang.wordNet.relations.filter(_.arguments.size == 2).map(_.name) if name.startsWith(prefix))
            candidates.add(name)
        }

        offset = prefix.size
      } else if (lastRelationIndex < lastVariableIndex) {
        val prefix = buffer.substring(lastVariableIndex + 1)

        if (isIdentifier(prefix)) {
          for (name <- lang.bindings.setVariables.keys.toList.sorted if name.startsWith(prefix))
            candidates.add(name)
        }

        offset = prefix.size
      }
    }

    if (candidates.isEmpty) -1 else buffer.length() - offset
  }
}
