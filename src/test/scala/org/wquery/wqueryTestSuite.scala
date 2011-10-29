package org.wquery
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.testng.TestNGSuite
import org.testng.annotations.BeforeClass
import org.wquery.emitter.{PlainWQueryEmitter, WQueryEmitter}

object WQueryRuntime {
  val wquery = createWQuery
  val emitter = new PlainWQueryEmitter

  def createWQuery = WQuery.createInstance("src/main/assembly/template/samplenet.xml")
}

abstract class WQueryTestSuite extends TestNGSuite with ShouldMatchers {  
  var wquery: WQuery = null
  var emitter: WQueryEmitter = null        
  
  class Result {
    def of(query: String) = emitter.emit(wquery.execute(query))
  }
  
  val result = new Result
  
  @BeforeClass def setUp() {
    wquery = WQueryRuntime.wquery
    emitter = WQueryRuntime.emitter
  }   
}
