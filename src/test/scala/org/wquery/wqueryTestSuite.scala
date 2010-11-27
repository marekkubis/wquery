package org.wquery

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.testng.TestNGSuite
import org.wquery.emitter.WQueryEmitter
import org.wquery.emitter.PlainLineWQueryEmitter
import org.testng.annotations.BeforeClass

object WQueryRuntime {    
  val wquery = WQuery.getInstance(System.getProperty("wquery.test.wordnet"))
  val emitter = new PlainLineWQueryEmitter
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
