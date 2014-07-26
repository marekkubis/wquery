package org.wquery.update

import java.io.{BufferedOutputStream, FileInputStream, FileOutputStream}

import org.rogach.scallop.Scallop
import org.rogach.scallop.exceptions.{Help, ScallopException, Version}
import org.wquery.WQueryProperties
import org.wquery.compile.WCompile
import org.wquery.loader.WnLoader
import org.wquery.utils.Logging

object WUpdateMain {
  def main(args: Array[String]) {
    val opts = Scallop(args)
      .version("wupdate " + WQueryProperties.version + " " + WQueryProperties.copyright)
      .banner( """
                 |Executes a WUpdate command
                 |
                 |usage: wupdate [OPTIONS] [IFILE] [OFILE]
                 |
                 |Options:
                 | """.stripMargin)
      .opt[String]("command", short = 'c')
      .opt[Boolean]("help", short = 'h', descr = "Show help message")
      .opt[Boolean]("quiet", short = 'q', descr = "Silent mode")
      .opt[Boolean]("version", short = 'v', descr = "Show version")
      .trailArg[String](name = "IFILE", required = false)
      .trailArg[String](name = "OFILE", required = false)

    try {
      opts.verify

      if (opts[Boolean]("quiet"))
        Logging.tryDisableLoggers()

      val input = opts.get[String]("IFILE")
        .map(inputName => new FileInputStream(inputName))
        .getOrElse(System.in)

      val output = opts.get[String]("OFILE")
        .map(outputFileName => new BufferedOutputStream(new FileOutputStream(outputFileName)))
        .getOrElse(Console.out)

      val loader = new WnLoader
      val wordNet = loader.load(input)
      val wupdate = new WUpdate(wordNet)
      wupdate.execute(opts[String]("command"))

      val wcompile = new WCompile()

      wcompile.compile(wupdate.wordNet, output)
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
