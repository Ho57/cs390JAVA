import java.io.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.sql.*;
import java.util.*;
 
public class Servlet extends HttpServlet {
   //compile line - sudo javac -cp .:/opt/tomcat7/lib/servlet-api.jar Servlet.java
   @Override
   public void doGet(HttpServletRequest request, HttpServletResponse response)
         throws IOException, ServletException {
 
      // Set the response MIME type of the response message
      response.setContentType("text/html");
      // Allocate a output writer to write the response message into the network socket
      PrintWriter out = response.getWriter();
      String query = request.getParameter("query");
      String[] params = query.split(" ");
      Connection conn = null;
      Statement stmt = null;
      HashSet<Integer> hs = new HashSet<Integer>();
      /*
         q: a b
         a 1
         b 1
         c 1 
         a 2
         b 2
         a 3
         c 3

         1 2 3
      */
      // Write the response message, in an HTML page
      try {
         conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/crawler","root","songbird");
         //conn = DriverManager.getConnection(aa,bb,cc);
         if(params.length != 0){
            String sqlQuery = "select * from words where word LIKE '"+params[0]+"'";
            
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sqlQuery);
            while(rs.next()){
               int urlID = rs.getInt("urlid");
               if(!hs.contains(urlID))
                  hs.add(urlID);
            }
            stmt.close();
            for(int i = 1; i < params.length; i++){
               HashSet<Integer> tmp = new HashSet<Integer>();
               String sqlQuery2 = "select * from words where word LIKE '"+params[i]+"'";
               stmt = conn.createStatement();
               ResultSet rs2 = stmt.executeQuery(sqlQuery);
               while(rs2.next()){
                  int urlID = rs2.getInt("urlid");
                  if(!tmp.contains(urlID) && hs.contains(urlID))
                     tmp.add(urlID);
               }
               stmt.close();
               hs = tmp;
            }
         }
         out.println("<html>");
         out.println("<head><title>My Search Engine</title></head>");
         out.println("<body>");
         out.println("<h1>Search the Internet:</h1>");
         out.println("<form action=\"/SearchEngine/search\" method=\"get\">");
         out.println("<input name=\"query\" type=\"text\" size=\"80\">");
         out.println("<br><br>");
         out.println("<input type=\"submit\" value=\"Search\">");
         out.println("<hr>");

         out.println("<h2>Total "+hs.size()+" records found:</h2>");
         Iterator it = new hs.iterator();
         while(it.hasNext()){
            int urlID = it.next();
            String sqlQuery = "select * from urls where urlid = '"+urlID+"'";
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sqlQuery);
            if (rs.next()) {
               String url = rs.getString("url");
               String description = rs.getString("description");
               String imgSrc = rs.getString("image");
               out.println("<a >"+url+"</a>");
               out.println("<h1>Search the Internet:</h1>");
            }
            stmt.close();
         }
         out.println("</form>");
         out.println("</body>");
         out.println("</html>");
        
      }
      catch(Exception e){
         out.println("<html>");
         out.println("<head><title>ISSUE</title></head>");
         out.println("<body>");
         String trace = e.toString() + "\n";                     

         for (StackTraceElement e1 : e.getStackTrace()) {
             trace += "\t at " + e1.toString() + "\n";
         }   
         out.println("<h1>"+trace+"</h1>");
         out.println("<br><br>");
         out.println("<input type=\"submit\" value=\"Search\">");
         out.println("<hr>");


         out.println("</form>");
         out.println("</body>");
         out.println("</html>");
      } finally {
         out.close();  // Always close the output writer
      }
   }
}