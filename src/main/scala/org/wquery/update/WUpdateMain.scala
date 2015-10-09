package org.wquery.update

import java.io._

import jline.console.ConsoleReader
import org.rogach.scallop.Scallop
import org.wquery.emitter.WQueryEmitter
import org.wquery.lang.{WLanguage, WLanguageMain}
import org.wquery.printer.WnPrinter

object WUpdateMain extends WLanguageMain("WUpdate", new WUpdate(_)) {
  override def appendOptions(opts: Scallop) = {
    opts
      .opt[Boolean]("print", short = 'p', descr = "Print output of the executed commands to stderr")
      .opt[String]("update", short = 'u', descr = "Same as -c but prepends the 'update' keyword to the command", required = false)
      .trailArg[String](name = "IFILE", required = false,
        descr = "A  wordnet model as created by wcompile (read from stdin if not specified)")
      .trailArg[String](name = "OFILE", required = false,
        descr = "A file to store the wordnet model updated by executing the update commands (printed to stdout if not specified)")
  }

  def doMain(lang: WLanguage, output: OutputStream, emitter: WQueryEmitter, opts: Scallop) {
    val resultLog = if (opts[Boolean]("print")) Some(new BufferedWriter(new OutputStreamWriter(System.err)), emitter) else None

    opts.get[String]("file").foreach(executeCommandFile(lang, _, resultLog))
    opts.get[String]("command").foreach(executeCommand(opts, lang, _, resultLog))
    opts.get[String]("update").foreach(command => executeCommand(opts, lang, "update " + command, resultLog))

    if (opts[Boolean]("interactive"))
      executeInteractive(lang, "wupdate", new ConsoleReader(System.in, System.err), emitter)

    val printer = new WnPrinter()

    printer.print(lang.wordNet, output)
  }
}
