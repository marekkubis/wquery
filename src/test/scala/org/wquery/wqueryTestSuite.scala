package org.wquery

import java.io.{FileInputStream, FileOutputStream}

import org.scalatest.Matchers
import org.scalatest.testng.TestNGSuite
import org.testng.annotations.BeforeClass
import org.wquery.emitter.{PlainWQueryEmitter, WQueryEmitter}
import org.wquery.lang.{Answer, Error}
import org.wquery.loader.WordNetLoader
import org.wquery.printer.WnPrinter
import org.wquery.update.WUpdate

object WQueryRuntime {
  val wquery = createWQuery
  val emitter = new PlainWQueryEmitter

  def createWQuery = {
    val printer = new WnPrinter
    val compiledWordNetPath = "target/samplenet.wn"
    val sourceNet = WordNetLoader.demandLoader("deb")
      .load(new FileInputStream("src/universal/samples/samplenet.xml"))
    printer.print(sourceNet, new FileOutputStream(compiledWordNetPath))

    val loadedWordNet = WordNetLoader.demandLoader("wn")
      .load(new FileInputStream(compiledWordNetPath))
    val wquery = new WUpdate(loadedWordNet)

    wquery.execute("\\hypernym transitivity := true")
    wquery.execute("\\partial_meronym transitivity := true")
    wquery.execute("\\member_meronym transitivity := true")
    wquery.execute("\\hypernym transitivity_action := `restore`")
    wquery.execute("\\partial_meronym transitivity_action := `restore`")
    wquery.execute("\\member_meronym transitivity_action := `restore`")
    wquery.execute("\\hypernym symmetry := `antisymmetric`")
    wquery.execute("\\hypernym symmetry_action := `preserve`")
    wquery.execute("\\similar symmetry := `symmetric`")
    wquery.execute("\\similar symmetry_action := `restore`")
    wquery.execute("\\desc^source required_by := true")
    wquery.execute("\\desc^source functional := true")
    wquery.execute("\\desc^source functional_action := `preserve`")

    wquery
  }
}

abstract class WQueryTestSuite extends TestNGSuite with Matchers {
  var wquery: WUpdate = null
  var emitter: WQueryEmitter = null

  class TuplesOf {
    def of(query: String) = wquery.execute(query) match {
      case Answer(_, ans) =>
        ans
      case Error(err) =>
        throw new IllegalArgumentException("Query returned error instead of dataset: " + err)
    }
  }

  val tuples = new TuplesOf

  class ResultOf {
    def of(query: String) = emitter.emit(wquery.execute(query, true, true))

    def withoutDistinctAndSort(query: String) = emitter.emit(wquery.execute(query, false, false))
  }

  val result = new ResultOf

  @BeforeClass def setUp() {
    wquery = WQueryRuntime.wquery
    emitter = WQueryRuntime.emitter
  }   
}
