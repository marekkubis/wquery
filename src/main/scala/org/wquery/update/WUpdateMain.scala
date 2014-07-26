package org.wquery.update

import java.io._

import org.rogach.scallop.Scallop
import org.wquery.compile.WCompile
import org.wquery.lang.WLanguageMain
import org.wquery.model.WordNet
import org.wquery.reader.{ExpressionReader, InputLineReader}

object WUpdateMain extends WLanguageMain("WUpdate") {
  override def appendOptions(opts: Scallop) = {
    opts.opt[Boolean]("emit", short = 'e',
      descr = "Emit output of the executed commands to stderr")
  }

  def doMain(wordNet: WordNet, output: OutputStream, opts: Scallop) {
    val wupdate = new WUpdate(wordNet)
    val emit = opts[Boolean]("emit")

    opts.get[String]("file").map { fileName =>
      val expressionReader = new ExpressionReader(new InputLineReader(new FileReader(fileName)))

      expressionReader.foreach { expr =>
        val result = wupdate.execute(expr)

        if (emit)
          System.err.print(emitter.emit(result))
      }

      expressionReader.close()
    }

    opts.get[String]("command").map { command =>
      val result = wupdate.execute(command)

      if (emit)
        System.err.print(emitter.emit(result))
    }

    val wcompile = new WCompile()

    wcompile.compile(wupdate.wordNet, output)
  }
}
