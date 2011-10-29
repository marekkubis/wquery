package org.wquery.console
import java.io.FileReader
import org.wquery.{WQueryProperties, WQuery}
import org.wquery.utils.Logging
import org.wquery.emitter.{RawWQueryEmitter, PlainWQueryEmitter}

object WQueryConsole extends Logging {
  def main(args: Array[String]) {
    if (args.isEmpty) {
      println("usage: wconsole wordnet.xml [queries.wq] ...")
      System.exit(1)
    }
    
    val raw = args.head == "-r"
    val fargs = if (raw) args.slice(1, args.size) else args
    val wquery = WQuery.createInstance(fargs.head)
    val emitter = if (raw) new RawWQueryEmitter else new PlainWQueryEmitter
    
    for (i <- 1 until fargs.size) {
      val qin = new QueryReader(new FileReader(fargs(i)))
      var query = ""
      
      do {
        query = qin.readQuery
        
        if (query != null && query.trim != "") {
          println(emitter.emit(wquery.execute(query)))       
        }
      } while (query != null)
        
      qin.close      
    }
    
    if (!raw) {
      println("WQuery (" + WQueryProperties.version + ") Console")
      println(WQueryProperties.copyright)      
    }
    
    val qin = new QueryReader(Console.in)    
    var query = ""
    
    do {
      if (!raw) {
        print("wquery> ")
      }
      
      query = qin.readQuery
      if (query != null && query.trim != "") {
        val result = wquery.execute(query)
        println(emitter.emit(result))
        debug(emitter.emit(result).replaceAll("\n","\\\\n").replaceAll("\t","\\\\t"))
      }
    } while (query != null)    
  }  
}
