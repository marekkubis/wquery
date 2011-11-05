package org.wquery.console
import org.wquery.{WQueryProperties, WQuery}
import org.wquery.utils.Logging
import org.clapper.argot.{ArgotUsageException, ArgotParser}
import org.clapper.argot.ArgotConverters._
import java.io.{FileReader, Reader}
import org.wquery.emitter.{WQueryEmitter, RawWQueryEmitter, PlainWQueryEmitter}

object WQueryConsole extends Logging {
  val WQueryBanner = "WQuery " + WQueryProperties.version + "\n" + WQueryProperties.copyright
  val argsParser = new ArgotParser("wquery", preUsage = Some(WQueryBanner))
  val quietOption = argsParser.flag[Boolean](List("q", "quiet"), "Silent mode")(convert = convertFlag)
  val wordnetParameter = argsParser.parameter[String]("wordnet", "Wordnet to be loaded", false)
  val queryParameter = argsParser.multiParameter[String]("queries", "File containing queries to be executed after loading the wordnet", true)

  def main(args: Array[String]) {
    try {
      argsParser.parse(args)

      val quiet = quietOption.value.getOrElse(false)
      val wquery = WQuery.createInstance(wordnetParameter.value.get)
      val emitter = if (quiet) new RawWQueryEmitter else new PlainWQueryEmitter

      for (queryFile <- queryParameter.value)
        readQueriesFromReader(new FileReader(queryFile), wquery, emitter, quiet)

      if (!quiet)
        println(WQueryBanner)

      readQueriesFromReader(Console.in, wquery, emitter, quiet)
    } catch {
      case e: ArgotUsageException =>
        println(e.message)
        System.exit(1)
    }
  }

  private def readQueriesFromReader(reader: Reader, wquery: WQuery, emitter: WQueryEmitter, quiet: Boolean) {
    val qin = new QueryReader(reader)

    while (!qin.isEof) {
      if (!quiet)
        print("wquery> ")

      qin.readQuery.map { query =>
        val result = wquery.execute(query)

        println(emitter.emit(result))
        debug(emitter.emit(result).replaceAll("\n","\\\\n").replaceAll("\t","\\\\t"))
      }
    }

    qin.close
  }

}
