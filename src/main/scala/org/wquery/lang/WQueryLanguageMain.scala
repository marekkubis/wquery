package org.wquery.lang

import java.io.{FileReader, OutputStream, OutputStreamWriter}

import org.rogach.scallop.Scallop
import org.wquery.model.WordNet
import org.wquery.reader.{ExpressionReader, InputLineReader}

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
    }

    opts.get[String]("command").map { command =>
      val result = lang.execute(command)
      writer.write(emitter.emit(result))
    }

    writer.close()
  }
}
