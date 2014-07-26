package org.wquery.compile

import java.io.{BufferedOutputStream, FileOutputStream}

import org.rogach.scallop._
import org.rogach.scallop.exceptions.{Help, ScallopException, Version}
import org.wquery.WQueryProperties
import org.wquery.loader.{DebLoader, LmfLoader}
import org.wquery.utils.Logging

object WCompileMain {
  def main(args: Array[String]) {
    val opts = Scallop(args)
      .version("wcompile " + WQueryProperties.version + " " + WQueryProperties.copyright)
      .banner( """
                 |Saves a wordnet loaded from a file in a binary representation
                 |
                 |usage: wcompile [OPTIONS] IFILE [OFILE]
                 |
                 |Options:
                 | """.stripMargin)
      .opt[Boolean]("help", short = 'h', descr = "Show help message")
      .opt[Boolean]("quiet", short = 'q', descr = "Silent mode")
      .opt[Boolean]("version", short = 'v', descr = "Show version")
      .opt[String]("type", short = 't', default = () => Some("deb"),
        validate = arg => List("deb", "lmf").contains(arg), descr = "Set input file type - either deb or lmf")
      .trailArg[String](name = "IFILE")
      .trailArg[String](name = "OFILE", required = false)

    try {
      opts.verify

      if (opts[Boolean]("quiet"))
        Logging.tryDisableLoggers()

      val loader = opts[String]("type") match {
        case "deb" =>
          new DebLoader()
        case "lmf" =>
          new LmfLoader()
      }

      val wordNet = loader.load(opts[String]("IFILE"))
      val wcompile = new WCompile
      val outputStream = opts.get[String]("OFILE")
        .map(outputFileName => new BufferedOutputStream(new FileOutputStream(outputFileName)))
        .getOrElse(Console.out)

      wcompile.compile(wordNet, outputStream)
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
