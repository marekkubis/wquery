package org.wquery.update

import java.io.{BufferedOutputStream, FileOutputStream}

import org.rogach.scallop.Scallop
import org.rogach.scallop.exceptions.ScallopException
import org.wquery.WQueryProperties
import org.wquery.compile.WCompile
import org.wquery.loader.WnLoader

object WUpdateMain {
  def main(args: Array[String]) {
    val opts = Scallop(args)
      .version("wupdate " + WQueryProperties.version)
      .banner( """usage: wupdate [OPTIONS] IFILE [OFILE]
                 |Executes a WUpdate command
                 |Options:
                 | """.stripMargin)
      .opt[String]("command", short = 'c')
      .trailArg[String](name = "IFILE")
      .trailArg[String](name = "OFILE", required = false)

    try {
      opts.verify

      val loader = new WnLoader
      val wordNet = loader.load(opts[String]("IFILE"))
      val wupdate = new WUpdate(wordNet)
      wupdate.execute(opts[String]("command"))

      val wcompile = new WCompile()
      val outputStream = opts.get[String]("OFILE")
        .map(outputFileName => new BufferedOutputStream(new FileOutputStream(outputFileName)))
        .getOrElse(Console.out)

      wcompile.compile(wupdate.wordNet, outputStream)
    } catch {
      case e: ScallopException =>
        println(e.message)
        opts.printHelp()
        sys.exit(1)
    }
  }

}
