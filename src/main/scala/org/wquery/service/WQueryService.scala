package org.wquery.service;
import java.io.{FileReader, File}
import org.mortbay.jetty.Server
import org.mortbay.jetty.servlet.{ServletHolder, Context}
import org.wquery.WQuery
import org.wquery.console.QueryReader
import org.wquery.emitter.PlainWQueryEmitter

object WQueryService {
    val emitter = new PlainWQueryEmitter
  
    def main(args: Array[String]) {
      if (args.isEmpty) {
          println("usage: wservice wordnet.xml [port] [queries.wq] ...")
          System.exit(1)
      }
      
      val wfile = args(0)      
      
      val (port, qargs) = if (args.size == 1) {
        (8010, Nil)
      } else {
        try {
          (args(1).toInt, if (args.size > 2) args.slice(2, args.size) else Nil)
        } catch {
          case _:java.lang.NumberFormatException =>
          (8010, if (args.size > 1) args.slice(1, args.size) else Nil)
        }
      }
      
      val wquery = WQuery.createInstance(wfile)
      
      qargs.asInstanceOf[List[String]].foreach {qarg =>
        val qin = new QueryReader(new FileReader(qarg))
          
        while(!qin.isEof)
          qin.readQuery.map(query => println(emitter.emit(wquery.execute(query))))
            
        qin.close            
      }
  
      val server = new Server(port)
      val root = new Context(server, "/", Context.NO_SESSIONS | Context.NO_SECURITY)
        
      root.addServlet(new ServletHolder(new WQueryServiceServlet(wquery)), "/wquery/" + new File(wfile).getName)          
      server.start()      
    }  
}
