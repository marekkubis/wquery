package org.wquery.console
import org.wquery.{WQueryProperties, WQuery}
import org.wquery.utils.Logging
import org.clapper.argot.{ArgotUsageException, ArgotParser}
import org.clapper.argot.ArgotConverters._
import java.io.{FileReader, Reader}
import org.wquery.emitter.{WQueryEmitter, RawWQueryEmitter, PlainWQueryEmitter}
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.{Logger, Level}

object WQueryConsole extends Logging {
  val WQueryBanner = "WQuery " + WQueryProperties.version + "\n" + WQueryProperties.copyright
  val argsParser = new ArgotParser("wquery", preUsage = Some(WQueryBanner))
  val quietOption = argsParser.flag[Boolean](List("q", "quiet"), "Silent mode")(convert = convertFlag)
  val emitterOption = argsParser.option[WQueryEmitter](List("e", "emitter"), "class", "Emitter class to be used (by default " + classOf[PlainWQueryEmitter].getName + ")") {
    (className, opt) =>
      try {
        Class.forName(className).newInstance.asInstanceOf[WQueryEmitter]
      } catch {
        case e: Exception =>
         argsParser.usage("Unable to load emitter " + className + " (" + e.getClass.getName + ").")
      }
  }
  val wordnetParameter = argsParser.parameter[String]("wordnet", "Wordnet to be loaded", false)
  val queryParameter = argsParser.multiParameter[String]("queries", "File containing queries to be executed after loading the wordnet", true)

  def main(args: Array[String]) {
    try {
      argsParser.parse(args)
      val quiet = quietOption.value.getOrElse(false)

      if (quiet)
        tryDisableLoggers

      val wquery = WQuery.createInstance(wordnetParameter.value.get)
      val emitter = emitterOption.value.getOrElse(new PlainWQueryEmitter)

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

  private def tryDisableLoggers {
    // slf4j - wquery and akka
    LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME) match {
      case logger: Logger =>
        logger.setLevel(Level.OFF)
      case logger =>
        logger.warn("Cannot disable the root logger in -q mode (the logger is not an instance of " + classOf[Logger].getName + ")")
    }

    // java.util.Logging - multiverse
    java.util.logging.Logger.getLogger("").setLevel(java.util.logging.Level.OFF);
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
