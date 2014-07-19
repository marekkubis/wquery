package org.wquery.compile

import java.io.{BufferedOutputStream, FileOutputStream}

import org.rogach.scallop._
import org.rogach.scallop.exceptions.ScallopException
import org.wquery.WQueryProperties
import org.wquery.loader.{DebLoader, LmfLoader}

object WCompileMain {
  def main(args: Array[String]) {
    val opts = Scallop(args)
      .version("wcompile " + WQueryProperties.version)
      .banner( """usage: wcompile [OPTIONS] IFILE [OFILE]
                 |Saves a wordnet loaded from a file in a binary representation
                 |Options:
                 | """.stripMargin)
      .opt[String]("type", short = 't', default = () => Some("deb"),
        validate = arg => List("deb", "lmf").contains(arg), descr = "Set input file type - either deb or lmf")
      .trailArg[String](name = "IFILE")
      .trailArg[String](name = "OFILE", required = false)

    try {
      opts.verify
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
      case e: ScallopException =>
        println(e.message)
        opts.printHelp()
        sys.exit(1)
    }
  }
}
