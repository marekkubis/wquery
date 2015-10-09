package org.wquery.lang

import jline.console.ConsoleReader
import org.wquery.emitter.WQueryEmitter
import org.wquery.reader.{ConsoleLineReader, ExpressionReader}

trait WInteractiveMain {
  def executeInteractive(lang: WLanguage, prompt: String, reader: ConsoleReader, emitter: WQueryEmitter): Unit = {
    reader.addCompleter(new WLanguageCompleter(lang.wordNet))
    reader.setExpandEvents(false)
    val expressionReader = new ExpressionReader(new ConsoleLineReader(reader, prompt + "> "))
    val writer = reader.getOutput

    expressionReader.foreach { expr =>
      val result = lang.execute(expr)
      writer.write(emitter.emit(result))
    }

    expressionReader.close()
  }
}
