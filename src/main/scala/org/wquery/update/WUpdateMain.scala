package org.wquery.update

import java.io._

import org.rogach.scallop.Scallop
import org.wquery.compile.WCompile
import org.wquery.lang.WLanguageMain
import org.wquery.model.WordNet
import org.wquery.reader.{ExpressionReader, InputLineReader}

object WUpdateMain extends WLanguageMain("WUpdate") {
  def doMain(wordNet: WordNet, output: OutputStream, opts: Scallop) {
    val wupdate = new WUpdate(wordNet)

    opts.get[String]("file").map { fileName =>
      val expressionReader = new ExpressionReader(new InputLineReader(new FileReader(fileName)))

      expressionReader.foreach { expr =>
        wupdate.execute(expr)
      }

      expressionReader.close()
    }

    opts.get[String]("command").map { command =>
      wupdate.execute(command)
    }

    val wcompile = new WCompile()

    wcompile.compile(wupdate.wordNet, output)
  }
}
