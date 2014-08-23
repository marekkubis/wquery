package org.wquery

import java.io.{FileInputStream, FileOutputStream}

import org.scalatest.Matchers
import org.scalatest.testng.TestNGSuite
import org.testng.annotations.BeforeClass
import org.wquery.emitter.{PlainWQueryEmitter, WQueryEmitter}
import org.wquery.lang.{Answer, Error, WLanguage}
import org.wquery.loader.WordNetLoader
import org.wquery.printer.WnPrinter
import org.wquery.update.WUpdate

object WQueryTestSuiteRuntime {
  val SampleNetPath = "src/universal/samples/samplenet.xml"
  val CompiledSampleNetPath = "target/samplenet.wn"
  val wquery = newWUpdate
  val emitter = new PlainWQueryEmitter

  compileSampleNet

  def compileSampleNet = {
    val printer = new WnPrinter
    val sampleNet = WordNetLoader.demandLoader("deb").load(new FileInputStream(SampleNetPath))
    val wupdate = new WUpdate(sampleNet)

    // add some antonyms
    wupdate.execute( """
                       |do
                       |  update relations += `antonym`
                       |  update `antonym` arguments += `src`, `sense`, 0
                       |  update `antonym` arguments += `dst`, `sense`, 1
                       |  update generated:2:n antonym := generated:1:n
                       |  update generated:3:n antonym := generated:1:n
                       |end
                     """.stripMargin)

    printer.print(wupdate.wordNet, new FileOutputStream(CompiledSampleNetPath))
  }

  def newWUpdate = {
    val sampleNet = WordNetLoader.demandLoader("wn")
      .load(new FileInputStream(CompiledSampleNetPath))
    new WUpdate(sampleNet)
  }
}

abstract class WQueryTestSuite extends TestNGSuite with Matchers {
  var wupdate: WUpdate = null
  var emitter: WQueryEmitter = null
  var result: ResultOf = null

  class TuplesOf {
    def of(query: String) = wupdate.execute(query) match {
      case Answer(_, ans) =>
        ans
      case Error(err) =>
        throw new IllegalArgumentException("Query returned error instead of dataset: " + err)
    }
  }

  val tuples = new TuplesOf

  class ResultOf(wlang: WLanguage) {
    def of(query: String) = emitter.emit(wlang.execute(query, true, true))

    def withoutDistinctAndSort(query: String) = emitter.emit(wlang.execute(query, false, false))

    def apply(wlang: WLanguage) = new ResultOf(wlang)
  }

  @BeforeClass def setUp() {
    wupdate = WQueryTestSuiteRuntime.wquery
    emitter = WQueryTestSuiteRuntime.emitter
    result = new ResultOf(wupdate)
  }   
}
