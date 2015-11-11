package org.wquery.similarity

import java.io._

import org.rogach.scallop._
import org.rogach.scallop.exceptions.{Help, ScallopException, Version}
import org.wquery.loader.WnLoader
import org.wquery.{WQueryCommandLineException, WQueryProperties}

import scala.collection.mutable
import scala.io.Source

object WTagMain {
  val loader = new WnLoader

  def loadBaseForms(fileName: String) = {
    val baseForms = mutable.Map[String, String]()

    for (line <- Source.fromFile(fileName).getLines() if !line.startsWith("#")) {
      val fields = line.split('\t')

      if (fields.length > 1) {
        baseForms(fields(0)) = fields(1)
      }
    }

    baseForms.toMap
  }

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
      .opt[String]("base-forms", short = 'b', descr = "base forms to be looked up in the wordnet for given surface forms (tab-separated list of pairs)", required = false)
      .opt[String]("field-separator", short = 'F', descr = "Set field separator", default = () => Some("\t"), required = false)
      .opt[Boolean]("help", short = 'h', descr = "Show help message")
      .opt[Boolean]("lowercase-lookup", short = 'L', descr = "Take into account the lowercase forms of words while searching the wordnet", required = false)
      .opt[Int]("max-compound-size", short = 'M', descr = "Max compound size", required = false)
      .opt[Int]("max-sense-count", short = 's', descr = "Use at most first <arg> senses to annotate text", required = false)
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
      val maxSenseCount = opts.get[Int]("max-sense-count")
      val baseForms = opts.get[String]("base-forms").map(loadBaseForms(_)).getOrElse(Map())

      val wordNetInput = opts.get[String]("WORDNET")
        .map(inputName => new FileInputStream(inputName))
        .getOrElse(System.in)

      val wordNet = loader.load(wordNetInput)

      val input = new BufferedInputStream(opts.get[String]("IFILE")
        .map(ifile => new FileInputStream(ifile)).getOrElse(System.in))

      val output = opts.get[String]("OFILE")
        .map(outputName => new FileOutputStream(outputName)).getOrElse(System.out)

      val tagger = new WTagger(wordNet, maxCompoundSize, maxSenseCount, baseForms, lowercaseLookup, separator)

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
