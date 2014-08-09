package org.wquery.printer

import java.io.{BufferedOutputStream, FileInputStream, FileOutputStream}

import org.rogach.scallop._
import org.rogach.scallop.exceptions.{Help, ScallopException, Version}
import org.wquery.WQueryProperties
import org.wquery.loader.WnLoader
import org.wquery.utils.Logging

object WPrintMain {
  def main(args: Array[String]) {
    val opts = Scallop(args)
      .version("wprint " + WQueryProperties.version + " " + WQueryProperties.copyright)
      .banner( """
                 |Prints a wordnet model in a specified format
                 |
                 |usage:
                 |
                 |  wprint [OPTIONS] [IFILE] [OFILE]
                 |
                 |options:
                 | """.stripMargin)
      .opt[Boolean]("help", short = 'h', descr = "Show help message")
      .opt[Boolean]("quiet", short = 'q', descr = "Silent mode")
      .opt[Boolean]("version", short = 'v', descr = "Show version")
      .opt[String]("type", short = 't', default = () => Some("deb"),
        validate = arg => WordNetPrinter.printers.contains(arg),
        descr = "Set output file format - either deb or lmf")
      .trailArg[String](name = "IFILE", required = false,
        descr = "A  wordnet model as created by wcompile (read from stdin if not specified)")
      .trailArg[String](name = "OFILE", required = false,
        descr = "A file to print the wordnet to (printed to stdout if not specified)")

    try {
      opts.verify

      if (opts[Boolean]("quiet"))
        Logging.tryDisableLoggers()

      val loader = new WnLoader
      val printer = WordNetPrinter.demandPrinter(opts[String]("type"))
      val input = opts.get[String]("IFILE")
        .map(ifile => new FileInputStream(ifile))
        .getOrElse(System.in)
      val wordNet = loader.load(input)
      val outputStream = opts.get[String]("OFILE")
        .map(outputFileName => new BufferedOutputStream(new FileOutputStream(outputFileName)))
        .getOrElse(Console.out)

      printer.print(wordNet, outputStream)
      outputStream.close()
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
