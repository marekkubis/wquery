package org.wquery.console

import org.rogach.scallop._
import org.rogach.scallop.exceptions.ScallopException
import org.wquery.WQueryProperties
import org.wquery.loader.GridLoader
import org.wquery.model.impl.InMemoryWordNet

object WCompileMain {
  def main(args: Array[String]) {
    val opts = Scallop(args)
      .version("wcompile " + WQueryProperties.version)
      .banner( """usage: wcompile [OPTIONS] IFILE [OFILE]
                 |Saves a wordnet loaded from a file in a binary representation
                 |Options:
                 | """.stripMargin)
      .opt[String]("type", short = 't', default = () => Some("deb"))
      .trailArg[String](name = "IFILE")
      .trailArg[String](name = "OFILE")

    try {
      opts.verify
      val loader = new GridLoader
      val wordNet = new InMemoryWordNet
      wordNet.open(opts[String]("OFILE"))
      loader.load(opts[String]("IFILE"), wordNet)
      wordNet.close()
    } catch {
      case e: ScallopException =>
        println(e.message)
        opts.printHelp()
        sys.exit(1)
    }
  }
}
