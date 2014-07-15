package org.wquery.lang

import org.rogach.scallop.Scallop
import org.rogach.scallop.exceptions.ScallopException
import org.wquery.WQueryProperties
import org.wquery.emitter.PlainWQueryEmitter
import org.wquery.loader.WnLoader
import org.wquery.model.WordNet

abstract class WLanguageMain {
  def languageName: String

  def language(wordNet: WordNet): WLanguage

  def commandName = languageName.toLowerCase

  def main(args: Array[String]) {
    val opts = Scallop(args)
      .version(commandName + " " + WQueryProperties.version)
      .banner( """usage: ${commandName} [OPTIONS] IFILE [OFILE]
                 |Executes a ${languageName} query
                 |Options:
                 | """.stripMargin)
      .opt[String]("command", short = 'c')
      .trailArg[String](name = "IFILE")
      .trailArg[String](name = "OFILE", required = false)

    try {
      opts.verify

      val loader = new WnLoader
      val wordNet = loader.load(opts[String]("IFILE"))
      val lang = language(wordNet)
      val result = lang.execute(opts[String]("command"))
      val emitter = new PlainWQueryEmitter

      println(emitter.emit(result))
    } catch {
      case e: ScallopException =>
        println(e.message)
        opts.printHelp()
        sys.exit(1)
    }
  }

}
