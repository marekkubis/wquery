package org.wquery.console
import java.io.{FileReader, BufferedReader}
import org.wquery.{WQueryProperties, WQuery}
import org.wquery.emitter.PlainLineWQueryEmitter
import org.wquery.utils.Logging

object WQueryConsole extends Logging {
  def main(args: Array[String]) {
    if (args.isEmpty) {
      println("usage: wconsole wordnet.xml [queries.wq] ...")
      System.exit(1)
    }
    
    val quiet = args.head == "-q"
    val fargs = if (quiet) args.slice(1, args.size) else args
    val wquery = WQuery.getInstance(fargs.head)
    val emitter = new PlainLineWQueryEmitter
    
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
    
    if (!quiet) {
      println("WQuery (" + WQueryProperties.version + ") Console")
      println(WQueryProperties.copyright)      
    }
    
    val qin = new QueryReader(Console.in)    
    var query = ""
    
    do {
      if (!quiet) {
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
