package org.wquery.lang

import java.io.{FileReader, OutputStream, OutputStreamWriter}

import jline.console.ConsoleReader
import org.rogach.scallop.Scallop
import org.wquery.model.WordNet
import org.wquery.reader.{ConsoleLineReader, ExpressionReader, InputLineReader}

class WQueryLanguageMain(languageName: String, language: WordNet => WLanguage) extends WLanguageMain(languageName) {
  def doMain(wordNet: WordNet, output: OutputStream, opts: Scallop) {
    val lang = language(wordNet)
    val writer = new OutputStreamWriter(output)

    opts.get[String]("file").map { fileName =>
      val expressionReader = new ExpressionReader(new InputLineReader(new FileReader(fileName)))

      expressionReader.foreach { expr =>
        val result = lang.execute(expr)
        writer.write(emitter.emit(result))
      }

      expressionReader.close()
      writer.flush()
    }

    opts.get[String]("command").map { command =>
      val result = lang.execute(command)
      writer.write(emitter.emit(result))
      writer.flush()
    }

    if (opts[Boolean]("interactive")) {
      val reader = new ConsoleReader(System.in, output)
      val expressionReader = new ExpressionReader(new ConsoleLineReader(reader))

      reader.setPrompt(languageName.toLowerCase + "> ")
      val writer = reader.getOutput

      expressionReader.foreach { expr =>
        val result = lang.execute(expr)
        writer.write(emitter.emit(result))
      }

      expressionReader.close()
    }

    writer.close()
  }
}
