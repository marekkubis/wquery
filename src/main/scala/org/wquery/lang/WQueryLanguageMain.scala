package org.wquery.lang

import java.io.{BufferedWriter, FileReader, OutputStream, OutputStreamWriter}

import jline.console.ConsoleReader
import org.rogach.scallop.Scallop
import org.wquery.emitter.WQueryEmitter
import org.wquery.lang.operations.AsTupleFunction
import org.wquery.model.{DataSet, WordNet}
import org.wquery.reader.{ConsoleLineReader, ExpressionReader, InputLineReader}

class WQueryLanguageMain(languageName: String, language: WordNet => WLanguage) extends WLanguageMain(languageName) {
  override def appendOptions(opts: Scallop) = {
    opts
      .trailArg[String](name = "IFILE", required = false,
        descr = "A wordnet model as created by wcompile (read from stdin if not specified)")
      .trailArg[String](name = "OFILE", required = false,
        descr = "A file to store query results (printed to stdout if not specified)")
  }

  def doMain(wordNet: WordNet, output: OutputStream, emitter: WQueryEmitter, opts: Scallop) {
    val lang = language(wordNet)
    val writer = new BufferedWriter(new OutputStreamWriter(output))

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
      if (opts[Boolean]("loop")) {
        for (line <- scala.io.Source.fromInputStream(System.in).getLines()) {
          val dataSet = if (opts[Boolean]("analyze")) {
            DataSet.fromTuple(AsTupleFunction.asTuple(wordNet, line, opts[String]("field-separator")))
          } else {
            DataSet.fromValue(line)
          }

          lang.bindSetVariable("D", dataSet)
          val result = lang.execute(command)
          writer.write(emitter.emit(result))
        }

        writer.flush()
      } else {
        val result = lang.execute(command)
        writer.write(emitter.emit(result))
        writer.flush()
      }
    }

    if (opts[Boolean]("interactive")) {
      val reader = new ConsoleReader(System.in, output)
      reader.addCompleter(new WLanguageCompleter(wordNet))
      val expressionReader = new ExpressionReader(new ConsoleLineReader(reader, languageName.toLowerCase + "> "))
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
