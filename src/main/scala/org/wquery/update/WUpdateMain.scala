package org.wquery.update

import java.io._

import jline.console.ConsoleReader
import org.rogach.scallop.Scallop
import org.wquery.lang.{WLanguageCompleter, WLanguageMain}
import org.wquery.model.WordNet
import org.wquery.printer.WnPrinter
import org.wquery.reader.{ConsoleLineReader, ExpressionReader, InputLineReader}

object WUpdateMain extends WLanguageMain("WUpdate") {
  override def appendOptions(opts: Scallop) = {
    opts
      .opt[Boolean]("emit", short = 'e', descr = "Emit output of the executed commands to stderr")
      .opt[String]("update", short = 'u', descr = "Same as -c but prepends the 'update' keyword to the command", required = false)
      .trailArg[String](name = "IFILE", required = false,
        descr = "A  wordnet model as created by wcompile (read from stdin if not specified)")
      .trailArg[String](name = "OFILE", required = false,
        descr = "A file to store the wordnet model updated by executing the update commands (printed to stdout if not specified)")
  }

  def doMain(wordNet: WordNet, output: OutputStream, opts: Scallop) {
    val wupdate = new WUpdate(wordNet)
    val emitMode = opts[Boolean]("emit")

    opts.get[String]("file").map { fileName =>
      val expressionReader = new ExpressionReader(new InputLineReader(new FileReader(fileName)))

      expressionReader.foreach { expr =>
        val result = wupdate.execute(expr)

        if (emitMode)
          System.err.print(emitter.emit(result))
      }

      expressionReader.close()
    }

    opts.get[String]("command").map { command =>
      val result = wupdate.execute(command)

      if (emitMode)
        System.err.print(emitter.emit(result))
    }

    opts.get[String]("update").map { command =>
      val result = wupdate.execute("update " + command)

      if (emitMode)
        System.err.print(emitter.emit(result))
    }

    if (opts[Boolean]("interactive")) {
      val reader = new ConsoleReader(System.in, System.err)
      reader.addCompleter(new WLanguageCompleter(wordNet))
      val expressionReader = new ExpressionReader(new ConsoleLineReader(reader, "wupdate> "))
      val writer = reader.getOutput

      expressionReader.foreach { expr =>
        val result = wupdate.execute(expr)
        writer.write(emitter.emit(result))
      }

      expressionReader.close()
    }

    val printer = new WnPrinter()

    printer.print(wupdate.wordNet, output)
  }
}
