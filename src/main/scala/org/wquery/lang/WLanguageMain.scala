package org.wquery.lang

import java.io._

import org.rogach.scallop.Scallop
import org.rogach.scallop.exceptions.{Help, ScallopException, Version}
import org.wquery.emitter.WQueryEmitter
import org.wquery.loader.WnLoader
import org.wquery.model.WordNet
import org.wquery.utils.Logging
import org.wquery.{WQueryCommandLineException, WQueryProperties}

abstract class WLanguageMain(languageName: String) {
  val loader = new WnLoader

  def doMain(wordNet: WordNet, output: OutputStream, emitter: WQueryEmitter, opts: Scallop)

  def appendOptions(opts: Scallop) = opts

  def main(args: Array[String]) {
    val commandName = languageName.toLowerCase
    val coreOpts = Scallop(args)
      .version(commandName + " " + WQueryProperties.version + " " + WQueryProperties.copyright)
      .banner(s"""
                 |Executes a $languageName command
                 |
                 |usage:
                 |
                 |  $commandName [OPTIONS] [IFILE] [OFILE]
                 |
                 |options:
                 | """.stripMargin)
      .opt[Boolean]("analyze", short = 'a', descr = "Analyze input line (to be used with -l option)", required = false)
      .opt[String]("command", short = 'c', descr = "Execute a command", required = false)
      .opt[String]("file", short = 'f', descr = "Execute commands from a file", required = false)
      .opt[String]("field-separator", short = 'F', descr = "Set field separator for -a option", default = () => Some("\t"), required = false)
      .opt[Boolean]("interactive", short = 'i', descr = "Run in the interactive interpreter mode", required = false)
      .opt[Boolean]("loop", short = 'l', descr = "Loop over stdin, pass every line of input as variable %D to -c command", required = false)
      .opt[Boolean]("help", short = 'h', descr = "Show help message")
      .opt[Boolean]("quiet", short = 'q', descr = "Silent mode")
      .opt[String]("emitter", short = 'e', default = () => Some("plain"),
        validate = arg => WQueryEmitter.emitters.contains(arg),
        descr = "Set result emitter (i.e. output format) - either raw, plain or escaping")
      .opt[Boolean]("version", short = 'v', descr = "Show version")

    val opts = appendOptions(coreOpts)

    try {
      opts.verify
      val interactiveMode = opts[Boolean]("interactive")
      val loopMode = opts[Boolean]("loop")

      if (opts[Boolean]("quiet"))
        Logging.tryDisableLoggers()

      val input = opts.get[String]("IFILE")
        .map(inputName => new FileInputStream(inputName))
        .getOrElse(
          if (interactiveMode)
            throw new WQueryCommandLineException("IFILE has to be specified in the interactive mode")
          else if (loopMode)
            throw new WQueryCommandLineException("IFILE has to be specified in the loop mode")
          else
            System.in
        )

      val output = opts.get[String]("OFILE")
        .map(outputName => new BufferedOutputStream(new FileOutputStream(outputName)))
        .getOrElse(System.out)

      val wordNet = loader.load(input)
      val emitter = WQueryEmitter.demandEmitter(opts[String]("emitter"))

      doMain(wordNet, output, emitter, opts)
      output.close()
    } catch {
      case e: Help =>
        opts.printHelp()
      case Version =>
        opts.printHelp()
      case e: ScallopException =>
        println("ERROR: " + e.message)
        println()
        opts.printHelp()
        sys.exit(1)
      case e: WQueryCommandLineException =>
        println("ERROR: " + e.message)
        println()
        opts.printHelp()
        sys.exit(1)
    }
  }
}
