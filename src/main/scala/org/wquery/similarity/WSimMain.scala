package org.wquery.similarity

import java.io._

import org.rogach.scallop._
import org.rogach.scallop.exceptions.{Help, ScallopException, Version}
import org.wquery.WQueryProperties
import org.wquery.emitter.TsvWQueryEmitter
import org.wquery.lang.WTupleParsers
import org.wquery.loader.WnLoader
import org.wquery.model._
import org.wquery.update.WUpdate

import scala.io.Source

object WSimMain {
  val loader = new WnLoader
  val emitter = new TsvWQueryEmitter

  def loadCounts(wordNet: WordNet, fileName: String): Unit = {
    val tupleParsers = new Object with WTupleParsers
    val senseCountRelation = Relation.binary("count", SenseType, IntegerType)
    val wordCountRelation = Relation.binary("count", StringType, IntegerType)

    wordNet.addRelation(senseCountRelation)
    wordNet.addRelation(wordCountRelation)

    for (line <- Source.fromFile(fileName).getLines()) {
      if (!line.startsWith("#")) {
        tupleParsers.parse(wordNet, line) match {
          case List(sense: Sense, count: Int) =>
            wordNet.addSuccessor(sense, senseCountRelation, count)
          case List(word: String, count: Int) =>
            wordNet.addSuccessor(word, wordCountRelation, count)
          case _ =>
            /* do nothing - count for an unknown word or sense provided */
        }
      }
    }
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
      .opt[String]("field-separator", short = 'F', descr = "Set field separator", default = () => Some("\t"), required = false)
      .opt[Boolean]("help", short = 'h', descr = "Show help message")
      .opt[String]("measure", short = 'm', default = () => Some("path"),
        descr = "Similarity measure")
      .opt[Boolean]("print-pairs", short = 'p', descr = "Print word/sense pairs to the output", required = false)
      .opt[Boolean]("version", short = 'v', descr = "Show version")
      .trailArg[String](name = "WORDNET", required = false,
        descr = "A wordnet model as created by wcompile (read from stdin if not specified)")
      .trailArg[String](name = "IFILE", required = false,
        descr = "Tab separated pairs of words, senses or synsets (read from stdin if not specified)")
      .trailArg[String](name = "OFILE", required = false,
        descr = "Similarity values (printed to stdout if not specified)")

    try {
      opts.verify

      val separator = opts[String]("field-separator")
      val measure = opts[String]("measure")
      val printPairs = opts[Boolean]("print-pairs")

      val wordNetInput = opts.get[String]("WORDNET")
        .map(inputName => new FileInputStream(inputName))
        .getOrElse(System.in)

      val wordNet = loader.load(wordNetInput)

      opts.get[String]("counts").foreach(fileName => loadCounts(wordNet, fileName))

      val wupdate = new WUpdate(wordNet)

      val input = opts.get[String]("IFILE")
        .map(ifile => new FileInputStream(ifile))
        .getOrElse(System.in)

      val output = opts.get[String]("OFILE")
        .map(outputName => new BufferedOutputStream(new FileOutputStream(outputName)))
        .getOrElse(System.out)

      val writer = new BufferedWriter(new OutputStreamWriter(output))

      wupdate.execute(
        """
          |function min_size do
          |  emit distinct(min(size(%A)))
          |end
        """.stripMargin)

      wupdate.execute(
        """
          |function lcs do
          |  %l, %r := %A
          |  %lh := last(%l.hypernym*)
          |  %rh := last(%r.hypernym*)
          |  %m := maxby((%lh intersect %rh)$a<$a,size($a.hypernym*[empty(hypernym)])>,2)
          |  emit as_synset(distinct(%m$a$_<$a>))
          |end
        """.stripMargin)

      wupdate.execute(
        """
          |function min_path_length do
          |  %l, %r := %A
          |  %lcs := lcs(%l, %r)
          |
          |  %ll := min_size(%l.hypernym*.%lcs)
          |  %rl := min_size(%r.hypernym*.%lcs)
          |  emit %ll + %rl - 1
          |end
        """.stripMargin)

      wupdate.execute(
        """
          |function path_measure do
          |  emit 1/min_path_length(%A)
          |end
        """.stripMargin)

      wupdate.execute(
        """
          |function max_size do
          |  emit distinct(max(size(%A)))
          |end
        """.stripMargin)

      wupdate.execute(
        """
          |function hierarchy_depth do
          |  %lcs := lcs(%A)
          |  %root := last(%lcs.hypernym*[empty(hypernym)])
          |  emit tree_depth(%root, `hypernym`)
          |end
        """.stripMargin)

      wupdate.execute(
        """
          |function lch_measure do
          |  emit max(-log(min_path_length(%A)/(2*hierarchy_depth(%A))))
          |end
        """.stripMargin)

      wupdate.execute(
        """
          |function root_dist do
          |  emit min_size(%A.hypernym*[empty(hypernym)]) + 1
          |end
        """.stripMargin)

      wupdate.execute(
        """
          |function lcs_dist do
          |  %s, %lcs := %A
          |  emit min_size(%s.hypernym*.%lcs) - 1
          |end
        """.stripMargin)

      wupdate.execute(
        """
          |function wup_measure do
          |  %l, %r := %A
          |  %lcs := lcs(%l, %r)
          |  %dl := lcs_dist(%l, %lcs)
          |  %dr := lcs_dist(%r, %lcs)
          |  %dlcs := root_dist(%lcs)
          |  emit 2*%dlcs/(%dl + %dr + 2*%dlcs)
          |end
        """.stripMargin)

      wupdate.execute(
        """
          |function tree_count emit sum(last(%A.^hypernym*.senses.word.count))
        """.stripMargin)

      wupdate.execute(
        """
          |function ic do
          |  %c := tree_count(%A)
          |  %d := sum(last(''.count))
          |  emit -log(%c/%d)
          |end
        """.stripMargin)

      wupdate.execute(
        """
          |function res_measure do
          |  %l, %r := %A
          |  %lcs := lcs(%l, %r)
          |  emit ic(%lcs)
          |end
        """.stripMargin)

      wupdate.execute(
        """
          |function jcn_measure do
          |  %l, %r := %A
          |  %lcs := lcs(%l, %r)
          |  emit 1/(ic(%l) + ic(%r) - 2*ic(%lcs))
          |end
        """.stripMargin)

      wupdate.execute(
        """
          |function lin_measure do
          |  %l, %r := %A
          |  %lcs := lcs(%l, %r)
          |  emit 2*ic(%lcs)/(ic(%l) + ic(%r))
          |end
        """.stripMargin)

      for (line <- scala.io.Source.fromInputStream(input).getLines()) {
        val result = wupdate.execute(
          s"""
            |do
            |  %l, %r := as_tuple(`$line`, `/$separator/`)
            |  emit na(distinct(max(from ({%l},{%r})$$a$$b emit ${measure}_measure($$a,$$b))))
            |end
          """.stripMargin)

        if (printPairs) {
          writer.write(line)
          writer.write(separator)
        }

        writer.write(emitter.emit(result))
        writer.flush()
      }

      writer.close()
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
