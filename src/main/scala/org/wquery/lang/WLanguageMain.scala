package org.wquery.lang

import java.io._

import jline.console.ConsoleReader
import org.rogach.scallop.Scallop
import org.rogach.scallop.exceptions.{Help, ScallopException, Version}
import org.wquery.emitter.WQueryEmitter
import org.wquery.lang.operations.AsTupleFunction
import org.wquery.loader.WnLoader
import org.wquery.model.{DataSet, WordNet}
import org.wquery.reader.{ConsoleLineReader, ExpressionReader, InputLineReader}
import org.wquery.utils.Logging
import org.wquery.{WQueryCommandLineException, WQueryProperties}

abstract class WLanguageMain(languageName: String, language: WordNet => WLanguage) {
  val loader = new WnLoader

  def doMain(lang: WLanguage, output: OutputStream, emitter: WQueryEmitter, opts: Scallop)

  def appendOptions(opts: Scallop) = opts

  def main(args: Array[String]) {
    val commandName = languageName.toLowerCase
    val coreOpts = Scallop(args)
      .version(commandName + " " + WQueryProperties.version + " " + WQueryProperties.copyright)
      .banner(s"""
                 |Executes a $languageName command
                 |
                 |usage:
                 |
                 |  $commandName [OPTIONS] [IFILE] [OFILE]
                 |
                 |options:
                 | """.stripMargin)
      .opt[Boolean]("analyze", short = 'a', descr = "Analyze input line (to be used with -l option)", required = false)
      .opt[String]("command", short = 'c', descr = "Execute a command", required = false)
      .opt[String]("file", short = 'f', descr = "Execute commands from a file", required = false)
      .opt[String]("field-separator", short = 'F', descr = "Set field separator for -a option", default = () => Some("\t"), required = false)
      .opt[Boolean]("interactive", short = 'i', descr = "Run in the interactive interpreter mode", required = false)
      .opt[Boolean]("loop", short = 'l', descr = "Loop over stdin, pass every line of input as variable %D to -c command", required = false)
      .opt[Boolean]("help", short = 'h', descr = "Show help message")
      .opt[Boolean]("quiet", short = 'q', descr = "Silent mode")
      .opt[String]("emitter", short = 'e', default = () => Some("plain"),
        validate = arg => WQueryEmitter.emitters.contains(arg),
        descr = "Set result emitter (i.e. output format) - either raw, plain or escaping")
      .opt[Boolean]("version", short = 'v', descr = "Show version")

    val opts = appendOptions(coreOpts)

    try {
      opts.verify
      val interactiveMode = opts[Boolean]("interactive")
      val loopMode = opts[Boolean]("loop")

      if (opts[Boolean]("quiet"))
        Logging.tryDisableLoggers()

      val input = opts.get[String]("IFILE")
        .map(inputName => new FileInputStream(inputName))
        .getOrElse(
          if (interactiveMode)
            throw new WQueryCommandLineException("IFILE has to be specified in the interactive mode")
          else if (loopMode)
            throw new WQueryCommandLineException("IFILE has to be specified in the loop mode")
          else
            System.in
        )

      val output = opts.get[String]("OFILE")
        .map(outputName => new BufferedOutputStream(new FileOutputStream(outputName)))
        .getOrElse(System.out)

      val lang = language(loader.load(input))
      val emitter = WQueryEmitter.demandEmitter(opts[String]("emitter"))

      doMain(lang, output, emitter, opts)
      output.close()
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

  def executeCommand(opts: Scallop, lang: WLanguage, command: String, resultLog: Option[(BufferedWriter, WQueryEmitter)]) {
    if (opts[Boolean]("loop")) {
      for (line <- scala.io.Source.fromInputStream(System.in).getLines()) {
        val dataSet = if (opts[Boolean]("analyze")) {
          DataSet.fromTuple(AsTupleFunction.asTuple(lang.wordNet, line, opts[String]("field-separator")))
        } else {
          DataSet.fromValue(line)
        }

        lang.bindSetVariable("D", dataSet)
        val result = lang.execute(command)
        resultLog.map{ case (writer, emitter) => writer.write(emitter.emit(result)) }
      }

      resultLog.map{ case (writer, _) => writer.flush()}
    } else {
      val result = lang.execute(command)
      resultLog.map{ case (writer, emitter) =>
        writer.write(emitter.emit(result))
        writer.flush()
      }
    }
  }

  def executeCommandFile(lang: WLanguage, fileName: String, resultLog: Option[(BufferedWriter, WQueryEmitter)]): Unit = {
    val expressionReader = new ExpressionReader(new InputLineReader(new FileReader(fileName)))

    expressionReader.foreach { expr =>
      val result = lang.execute(expr)
      resultLog.map{ case (writer, emitter) => writer.write(emitter.emit(result)) }
    }

    expressionReader.close()
    resultLog.map{ case (writer, _) => writer.flush() }
  }

  def executeInteractive(lang: WLanguage, reader: ConsoleReader, emitter: WQueryEmitter): Unit = {
    reader.addCompleter(new WLanguageCompleter(lang.wordNet))
    val expressionReader = new ExpressionReader(new ConsoleLineReader(reader, languageName.toLowerCase + "> "))
    val writer = reader.getOutput

    expressionReader.foreach { expr =>
      val result = lang.execute(expr)
      writer.write(emitter.emit(result))
    }

    expressionReader.close()
  }

}
