package org.wquery.compile

import java.io.{BufferedOutputStream, FileInputStream, FileOutputStream}

import org.rogach.scallop._
import org.rogach.scallop.exceptions.{Help, ScallopException, Version}
import org.wquery.WQueryProperties
import org.wquery.loader.WordNetLoader
import org.wquery.printer.WnPrinter
import org.wquery.utils.Logging

object WCompileMain {
  def main(args: Array[String]) {
    val opts = Scallop(args)
      .version("wcompile " + WQueryProperties.version + " " + WQueryProperties.copyright)
      .banner( """
                 |Creates a binary model of a wordnet
                 |
                 |usage:
                 |
                 |  wcompile [OPTIONS] [IFILE] [OFILE]
                 |
                 |options:
                 | """.stripMargin)
      .opt[Boolean]("help", short = 'h', descr = "Show help message")
      .opt[Boolean]("quiet", short = 'q', descr = "Silent mode")
      .opt[Boolean]("version", short = 'v', descr = "Show version")
      .opt[String]("type", short = 't', default = () => Some("deb"),
        validate = arg => WordNetLoader.loaders.contains(arg),
        descr = "Set input file type - either deb or lmf")
      .trailArg[String](name = "IFILE", required = false,
        descr = "A wordnet in the format specified by the -t option (read from stdin if not specified)")
      .trailArg[String](name = "OFILE", required = false,
        descr = "A file to store the wordnet model (printed to stdout if not specified)")

    try {
      opts.verify

      if (opts[Boolean]("quiet"))
        Logging.tryDisableLoggers()

      val loader = WordNetLoader.demandLoader(opts[String]("type"))
      val input = opts.get[String]("IFILE")
        .map(ifile => new FileInputStream(ifile))
        .getOrElse(System.in)
      val wordNet = loader.load(input)
      val printer = new WnPrinter
      val outputStream = opts.get[String]("OFILE")
        .map(outputFileName => new BufferedOutputStream(new FileOutputStream(outputFileName)))
        .getOrElse(Console.out)

      printer.print(wordNet, outputStream)
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
