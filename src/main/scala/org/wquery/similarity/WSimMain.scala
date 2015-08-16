package org.wquery.similarity

import java.io._

import org.rogach.scallop._
import org.rogach.scallop.exceptions.{Help, ScallopException, Version}
import org.wquery.WQueryProperties
import org.wquery.emitter.TsvWQueryEmitter
import org.wquery.loader.WnLoader
import org.wquery.update.WUpdate

object WSimMain {
  val loader = new WnLoader
  val emitter = new TsvWQueryEmitter

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
      .opt[Boolean]("help", short = 'h', descr = "Show help message")
      .opt[Boolean]("version", short = 'v', descr = "Show version")
      .opt[String]("measure", short = 'm', default = () => Some("path"),
        descr = "Similarity measure")
      .trailArg[String](name = "WORDNET", required = false,
        descr = "A wordnet model as created by wcompile (read from stdin if not specified)")
      .trailArg[String](name = "IFILE", required = false,
        descr = "Tab separated pairs of words, senses or synsets (read from stdin if not specified)")
      .trailArg[String](name = "OFILE", required = false,
        descr = "Similarity values (printed to stdout if not specified)")

    try {
      opts.verify

      val measure = opts[String]("measure")

      val wordNetInput = opts.get[String]("WORDNET")
        .map(inputName => new FileInputStream(inputName))
        .getOrElse(System.in)

      val wupdate = new WUpdate(loader.load(wordNetInput))

      val input = opts.get[String]("IFILE")
        .map(ifile => new FileInputStream(ifile))
        .getOrElse(System.in)

      val output = opts.get[String]("OFILE")
        .map(outputName => new BufferedOutputStream(new FileOutputStream(outputName)))
        .getOrElse(System.out)

      val writer = new BufferedWriter(new OutputStreamWriter(output))

      writer.write(emitter.emit(wupdate.execute(
        """
          |function min_path_length do
          |  %l := %A$a$_<$a>
          |  %r := %A$a<$a>
          |  %p := shortest({%l}.hypernym*.^hypernym*.{%r})
          |
          |  if [ empty(%p) ] do
          |    %ll := size(shortest({%l}.hypernym*[empty(hypernym)]))
          |    %rl := size(shortest({%r}.hypernym*[empty(hypernym)]))
          |    emit distinct(%ll + %rl + 1)
          |  end else
          |    emit distinct(size(%p))
          |end
        """.stripMargin)))

      writer.write(emitter.emit(wupdate.execute(
        """
          |function path_measure do
          |  emit 1/min_path_length(%A)
          |end
        """.stripMargin)))

      writer.write(emitter.emit(wupdate.execute(
        """
          |function lch_measure do
          |  %d := distinct(size(longest({}[empty(hypernym)].^hypernym*))) + 1
          |  emit max(-log(min_path_length(%A)/(2*%d)))
          |end
        """.stripMargin)))

      writer.write(emitter.emit(wupdate.execute(
        """
          |function lcs do
          |  %l := %A$a$_<$a>
          |  %r := %A$a<$a>
          |  %lh := last(%l.hypernym*)
          |  %rh := last(%r.hypernym*)
          |  emit maxby((%lh intersect %rh)$a<$a,size($a.hypernym*)>,2)$a$_<$a>
          |end
        """.stripMargin)))

      writer.write(emitter.emit(wupdate.execute(
        """
          |function wup_measure do
          |  %l := %A$a$_<$a>
          |  %r := %A$a<$a>
          |  %lh := last({%l}.hypernym*)
          |  %dl := distinct(min(size({%l}.hypernym*[empty(hypernym)])))
          |  %dr := distinct(min(size({%r}.hypernym*[empty(hypernym)])))
          |  %lcs := lcs(%l,%r)
          |  emit %dl,%dr,(lcs({%l},{%r})).hypernym
          |end
        """.stripMargin)))

      for (line <- scala.io.Source.fromInputStream(input).getLines()) {
        val result = wupdate.execute(measure + "_measure(as_tuple(`" + line + "`, `/ /`))")
        writer.write(emitter.emit(result))
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
