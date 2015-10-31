package org.wquery.similarity

import java.io._

import org.rogach.scallop._
import org.rogach.scallop.exceptions.{Help, ScallopException, Version}
import org.wquery.loader.WnLoader
import org.wquery.{WQueryCommandLineException, WQueryProperties}

object WTagMain {
  val loader = new WnLoader

  def main(args: Array[String]) {
    val opts = Scallop(args)
      .version("wtag " + WQueryProperties.version + " " + WQueryProperties.copyright)
      .banner( """
                 |Annotates words using the senses from the wordnet.
                 |
                 |usage:
                 |
                 |  wtag [OPTIONS] [WORDNET] [IFILE] [OFILE]
                 |
                 |options:
                 | """.stripMargin)
      .opt[String]("field-separator", short = 'F', descr = "Set field separator", default = () => Some("\t"), required = false)
      .opt[Boolean]("help", short = 'h', descr = "Show help message")
      .opt[Boolean]("lowercase-lookup", short = 'L', descr = "Take into account the lowercase forms of words while searching the wordnet", required = false)
      .opt[Int]("max-compound-size", short = 'M', descr = "Max compound size")
      .opt[Boolean]("version", short = 'v', descr = "Show version")
      .trailArg[String](name = "WORDNET", required = true,
        descr = "A wordnet model as created by wcompile")
      .trailArg[String](name = "IFILE", required = false,
        descr = "Tokenized text in the sentence per line format")
      .trailArg[String](name = "OFILE", required = false,
        descr = "Annotated text")

    try {
      opts.verify

      val lowercaseLookup = opts[Boolean]("lowercase-lookup")
      val separator = opts[String]("field-separator")
      val maxCompoundSize = opts.get[Int]("max-compound-size")

      val wordNetInput = opts.get[String]("WORDNET")
        .map(inputName => new FileInputStream(inputName))
        .getOrElse(System.in)

      val wordNet = loader.load(wordNetInput)

      val input = new BufferedInputStream(opts.get[String]("IFILE")
        .map(ifile => new FileInputStream(ifile)).getOrElse(System.in))

      val output = opts.get[String]("OFILE")
        .map(outputName => new FileOutputStream(outputName)).getOrElse(System.out)

      val tagger = new WTagger(wordNet, maxCompoundSize, lowercaseLookup, separator)

      tagger.tag(input, output)
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
