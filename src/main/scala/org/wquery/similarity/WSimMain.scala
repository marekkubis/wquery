package org.wquery.similarity

import java.io._

import jline.console.ConsoleReader
import org.rogach.scallop._
import org.rogach.scallop.exceptions.{Help, ScallopException, Version}
import org.wquery.emitter.WQueryEmitter
import org.wquery.lang.operations.{LowerFunction, TitleFunction, UpperFunction}
import org.wquery.lang.{WInteractiveMain, WTupleParsers}
import org.wquery.loader.WnLoader
import org.wquery.model._
import org.wquery.update.WUpdate
import org.wquery.{WQueryCommandLineException, WQueryProperties}

import scala.io.Source

object WSimMain extends WInteractiveMain {
  val loader = new WnLoader

  def loadCounts(wordNet: WordNet, fileName: String) = {
    val tupleParsers = new Object with WTupleParsers
    val senseCountRelation = Relation.binary("count", SenseType, IntegerType)
    val wordCountRelation = Relation.binary("count", StringType, IntegerType)
    var senseCounts = false

    wordNet.addRelation(senseCountRelation)
    wordNet.addRelation(wordCountRelation)

    for (line <- Source.fromFile(fileName).getLines()) {
      if (!line.startsWith("#")) {
        tupleParsers.parse(wordNet, line) match {
          case List(sense: Sense, count: Int) =>
            wordNet.addSuccessor(sense, senseCountRelation, count)
            senseCounts = true
          case List(word: String, count: Int) =>
            wordNet.addSuccessor(word, wordCountRelation, count)
          case _ =>
            /* do nothing - count for an unknown word or sense provided */
        }
      }
    }

    senseCounts
  }

  def main(args: Array[String]) {
    val opts = Scallop(args)
      .version("wsim " + WQueryProperties.version + " " + WQueryProperties.copyright)
      .banner( """
                 |Computes semantic similarity among pairs of words, senses and synsets.
                 |
                 |usage:
                 |
                 |  wsim [OPTIONS] [WORDNET] [IFILE] [OFILE]
                 |
                 |options:
                 | """.stripMargin)
      .opt[String]("counts", short = 'c', descr = "Word and/or sense counts for IC-based measures", required = false)
      .opt[String]("emitter", short = 'e', default = () => Some("tsv"),
        validate = arg => WQueryEmitter.emitters.contains(arg),
        descr = "Set result emitter (i.e. output format) - either raw, plain, escaping or tsv")
      .opt[String]("field-separator", short = 'F', descr = "Set field separator", default = () => Some("\t"), required = false)
      .opt[Boolean]("help", short = 'h', descr = "Show help message")
      .opt[Boolean]("ignore-case", short = 'I', descr = "Ignore case while looking up words in the wordnet", required = false)
      .opt[Boolean]("interactive", short = 'i', descr = "Run in the interactive interpreter mode", required = false)
      .opt[String]("measure", short = 'm', default = () => Some("path"),
        descr = "Similarity measure")
      .opt[Boolean]("print-pairs", short = 'p', descr = "Print word/sense pairs to the output", required = false)
      .opt[Boolean]("root-node", short = 'r', descr = "Introduce root nodes in the nouns and verbs hierarchies", required = false)
      .opt[Boolean]("version", short = 'v', descr = "Show version")
      .trailArg[String](name = "WORDNET", required = false,
        descr = "A wordnet model as created by wcompile (read from stdin if not specified)")
      .trailArg[String](name = "IFILE", required = false,
        descr = "Tab separated pairs of words, senses or synsets (read from stdin if not specified)")
      .trailArg[String](name = "OFILE", required = false,
        descr = "Similarity values (printed to stdout if not specified)")

    try {
      opts.verify

      val emitter = WQueryEmitter.demandEmitter(opts[String]("emitter"))
      val ignoreCase = opts[Boolean]("ignore-case")
      val interactiveMode = opts[Boolean]("interactive")
      val separator = opts[String]("field-separator")
      val measure = opts[String]("measure")
      val printPairs = opts[Boolean]("print-pairs")
      val rootNode = opts[Boolean]("root-node")

      val wordNetInput = opts.get[String]("WORDNET")
        .map(inputName => new FileInputStream(inputName))
        .getOrElse(System.in)

      val wordNet = loader.load(wordNetInput)
      wordNet.addRelation(Relation.binary("count", SynsetType, IntegerType))

      val senseCounts = opts.get[String]("counts").map(fileName => loadCounts(wordNet, fileName)).getOrElse(true)

      val wupdate = new WUpdate(wordNet)

      wupdate.execute("from !instance_hypernym$a$_$b update $a hypernym += $b")

      if (rootNode) {
        wupdate.execute("update synsets += {ROOT:1:n}")
        wupdate.execute("update {}[empty(hypernym) and pos = `n`] hypernym := {ROOT:1:n}")
        wupdate.execute("update {ROOT:1:n} pos := `n`")
        wupdate.execute("update synsets += {ROOT:1:v}")
        wupdate.execute("update {}[empty(hypernym) and pos = `v`] hypernym := {ROOT:1:v}")
        wupdate.execute("update {ROOT:1:v} pos := `v`")
      }

      if (measure != "path" && measure != "wup" && measure != "lch") {
        if (senseCounts) {
          wupdate.execute("from {}$a update $a count := sum(last($a.senses.count))")
        } else {
          wupdate.execute("from {}$a update $a count := sum(last($a.senses.word.count))")
        }
      }

      if (interactiveMode) {
        executeInteractive(wupdate, "wsim", new ConsoleReader(System.in, System.out), emitter)
      } else {
        def escape(x: String) = "as_tuple(`" + x + "`)"

        def caseIgnoringQuery(x: String) = List(escape(x), escape(UpperFunction.upper(x)),
          escape(LowerFunction.lower(x)), escape(TitleFunction.title(x))).mkString(" union ")

        val input = opts.get[String]("IFILE")
          .map(ifile => new FileInputStream(ifile)).getOrElse(System.in)

        val output = opts.get[String]("OFILE")
          .map(outputName => new BufferedOutputStream(new FileOutputStream(outputName))).getOrElse(System.out)

        val writer = new BufferedWriter(new OutputStreamWriter(output))

        for (line <- scala.io.Source.fromInputStream(input).getLines()) {
          val fields = line.split(separator, 2)
          val (left, right) = (fields(0), fields(1))

          val (leftSenseQuery, rightSenseQuery) = if (!ignoreCase)
            (escape(left), escape(right))
          else
            (caseIgnoringQuery(left), caseIgnoringQuery(right))

          val  pairQuery = if (printPairs) "`" + left + "`,`" + right + "`," else ""

          val result = wupdate.execute(
            s"""
               |do
               |  %l := {distinct($leftSenseQuery)}
               |  %r := {distinct($rightSenseQuery)}
               |  emit ${pairQuery}na(distinct(max(from (%l,%r)$$a$$b emit ${measure}_measure($$a,$$b))))
               |end
          """.stripMargin)

          writer.write(emitter.emit(result))
          writer.flush()
        }

        writer.close()
      }
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
