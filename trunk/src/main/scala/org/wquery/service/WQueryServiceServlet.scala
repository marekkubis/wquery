package org.wquery.service
import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}
import org.wquery.WQuery

class WQueryServiceServlet(wquery: WQuery) extends HttpServlet {
    val QueryParameter = "query"
    val emitter = new JSonEmitter
      
    override def doGet(request: HttpServletRequest, response: HttpServletResponse) {
      response.setContentType("application/json")
      response.setCharacterEncoding("UTF-8")

      val query = request.getParameter(QueryParameter)      
        
      if (query != null) {        
        response.getWriter().write(emitter.emit(wquery.execute(query)))
      } else {
        response.getWriter().write("{error:\"no query string found\"}")
      }
    }
}
