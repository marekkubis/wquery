package org.wquery.lang

import java.io.{BufferedWriter, OutputStream, OutputStreamWriter}

import jline.console.ConsoleReader
import org.rogach.scallop.Scallop
import org.wquery.emitter.WQueryEmitter
import org.wquery.model.WordNet

class WQueryLanguageMain(languageName: String, language: WordNet => WLanguage) extends WLanguageMain(languageName, language) {
  override def appendOptions(opts: Scallop) = {
    opts
      .trailArg[String](name = "IFILE", required = false,
        descr = "A wordnet model as created by wcompile (read from stdin if not specified)")
      .trailArg[String](name = "OFILE", required = false,
        descr = "A file to store query results (printed to stdout if not specified)")
  }

  def doMain(lang: WLanguage, output: OutputStream, emitter: WQueryEmitter, opts: Scallop) {
    val writer = new BufferedWriter(new OutputStreamWriter(output))
    val resultLog = Some(writer, emitter)

    opts.get[String]("file").foreach(executeCommandFile(lang, _, resultLog))
    opts.get[String]("command").foreach(executeCommand(opts, lang, _, resultLog))

    if (opts[Boolean]("interactive"))
      executeInteractive(lang, languageName.toLowerCase, new ConsoleReader(System.in, output), emitter)

    writer.close()
  }
}
