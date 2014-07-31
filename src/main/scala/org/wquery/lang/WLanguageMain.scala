package org.wquery.lang

import java.io._

import org.rogach.scallop.Scallop
import org.rogach.scallop.exceptions.{Help, ScallopException, Version}
import org.wquery.emitter.PlainWQueryEmitter
import org.wquery.loader.WnLoader
import org.wquery.model.WordNet
import org.wquery.utils.Logging
import org.wquery.{WQueryCommandLineException, WQueryProperties}

abstract class WLanguageMain(languageName: String) {
  val loader = new WnLoader
  val emitter = new PlainWQueryEmitter

  def doMain(wordNet: WordNet, output: OutputStream, opts: Scallop)

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
      .opt[String]("command", short = 'c', descr = "Execute a command", required = false)
      .opt[String]("file", short = 'f', descr = "Execute commands from a file", required = false)
      .opt[Boolean]("interactive", short = 'i', descr = "Run in the interactive interpreter mode", required = false)
      .opt[Boolean]("help", short = 'h', descr = "Show help message")
      .opt[Boolean]("quiet", short = 'q', descr = "Silent mode")
      .opt[Boolean]("version", short = 'v', descr = "Show version")

    val opts = appendOptions(coreOpts)

    try {
      opts.verify
      val interactiveMode = opts[Boolean]("interactive")

      if (opts[Boolean]("quiet"))
        Logging.tryDisableLoggers()

      val input = opts.get[String]("IFILE")
        .map(inputName => new FileInputStream(inputName))
        .getOrElse(
          if (interactiveMode)
            throw new WQueryCommandLineException("IFILE has to be specified in the interactive mode")
          else
            System.in
        )

      val output = opts.get[String]("OFILE")
        .map(outputName => new BufferedOutputStream(new FileOutputStream(outputName)))
        .getOrElse(System.out)

      val wordNet = loader.load(input)

      doMain(wordNet, output, opts)
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
