package org.wquery.lang

import org.rogach.scallop.Scallop
import org.rogach.scallop.exceptions.{Help, ScallopException, Version}
import org.wquery.WQueryProperties
import org.wquery.emitter.PlainWQueryEmitter
import org.wquery.loader.WnLoader
import org.wquery.model.WordNet
import org.wquery.utils.{FileUtils, Logging}

abstract class WLanguageMain {
  def languageName: String

  def language(wordNet: WordNet): WLanguage

  def commandName = languageName.toLowerCase

  def main(args: Array[String]) {
    val opts = Scallop(args)
      .version(commandName + " " + WQueryProperties.version + " " + WQueryProperties.copyright)
      .banner(s"""
                 |Executes a $languageName query
                 |
                 |usage: $commandName [OPTIONS] IFILE [OFILE]
                 |
                 |Options:
                 | """.stripMargin)
      .opt[String]("command", short = 'c')
      .opt[Boolean]("help", short = 'h', descr = "Show help message")
      .opt[Boolean]("quiet", short = 'q', descr = "Silent mode")
      .opt[Boolean]("version", short = 'v', descr = "Show version")
      .trailArg[String](name = "IFILE")
      .trailArg[String](name = "OFILE", required = false)

    try {
      opts.verify

      if (opts[Boolean]("quiet"))
        Logging.tryDisableLoggers()

      val loader = new WnLoader
      val wordNet = loader.load(opts[String]("IFILE"))
      val lang = language(wordNet)
      val result = lang.execute(opts[String]("command"))
      val emitter = new PlainWQueryEmitter
      val output = emitter.emit(result)

      opts.get[String]("OFILE")
        .map(outputFileName => FileUtils.dump(output, outputFileName))
        .getOrElse(print(output))
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
    }
  }

}
