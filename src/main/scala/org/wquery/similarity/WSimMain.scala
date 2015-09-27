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
      .opt[String]("field-separator", short = 'F', descr = "Set field separator", default = () => Some("\t"), required = false)
      .opt[Boolean]("help", short = 'h', descr = "Show help message")
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
        wupdate.execute("update synsets += {ROOT:1:v}")
        wupdate.execute("update {}[empty(hypernym) and pos = `v`] hypernym := {ROOT:1:v}")
      }

      if (senseCounts) {
        wupdate.execute("from {}$a update $a count := sum(last($a.senses.count))")
      } else {
        wupdate.execute("from {}$a update $a count := sum(last($a.senses.word.count))")
      }

      val input = opts.get[String]("IFILE")
        .map(ifile => new FileInputStream(ifile))
        .getOrElse(System.in)

      val output = opts.get[String]("OFILE")
        .map(outputName => new BufferedOutputStream(new FileOutputStream(outputName)))
        .getOrElse(System.out)

      val writer = new BufferedWriter(new OutputStreamWriter(output))

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
          |function ic do
          |  %s := as_synset(%A)
          |  %c := tree_sum(%s, `hypernym`, `count`)
          |  %root := last(%s.hypernym*[empty(hypernym)])
          |  %d := tree_sum(%root, `hypernym`, `count`)
          |  emit -log(%c/%d)
          |end
        """.stripMargin)

      wupdate.execute(
        """
          |function res_measure do
          |  %l, %r := %A
          |  %lcs := lcs(%l, %r)
          |
          |  emit ic(%lcs)
          |end
        """.stripMargin)

      wupdate.execute(
        """
          |function jcn_measure do
          |  %l, %r := %A
          |  %lcs := lcs(%l, %r)
          |  %dist := ic(%l) + ic(%r) - 2*ic(%lcs)
          |
          |  if [%dist = 0 or is_nan(%dist)] do
          |    %r := last(%lcs.hypernym*[empty(hypernym)])
          |    %d := distinct(max(tree_sum(%r, `hypernym`, `count`)))
          |
          |    if [%d > 0.01]
          |       emit 1/-log((%d - 0.01)/%d)
          |     else
          |       emit 0
          |  end else do
          |    emit 1/%dist
          |  end
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
