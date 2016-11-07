import java.io.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.sql.*;
import java.util.*;
 
// HTMLFilter class
class HTMLFilter{
    public static String filter(String message) {

        if (message == null)
            return (null);

        char content[] = new char[message.length()];
        message.getChars(0, message.length(), content, 0);
        StringBuffer result = new StringBuffer(content.length + 50);
        for (int i = 0; i < content.length; i++) {
            switch (content[i]) {
            case '<':
                result.append("&lt;");
                break;
            case '>':
                result.append("&gt;");
                break;
            case '&':
                result.append("&amp;");
                break;
            case '"':
                result.append("&quot;");
                break;
            case '\'':
                result.append("&apos;");
                break;
            case '\\':
                result.append("&bs;");
                break;
            default:
                result.append(content[i]);
            }
        }
        return (result.toString());

    }
}


public class Servlet extends HttpServlet {
   //compile line - sudo javac -cp .:../../../../lib/servlet-api.jar Servlet.java
   @Override
   public void doGet(HttpServletRequest request, HttpServletResponse response)
         throws IOException, ServletException {
 
      // Set the response MIME type of the response message
      response.setContentType("text/html");
      // Allocate a output writer to write the response message into the network socket
      PrintWriter out = response.getWriter();
      String query = request.getParameter("query");
      String[] params = query.split(" ");
      HTMLFilter htmlFilter = new HTMLFilter();
      for(int i = 0; i < params.length; i++){
         params[i] = htmlFilter.filter(params[i]);
      }
      Connection conn = null;
      Statement stmt = null;
      //HashSet<Integer> hs = new HashSet<Integer>();
      Vector<Integer> vec = new Vector<Integer>(10);

      // Write the response message, in an HTML page
      try {
         conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/crawler","root","songbird");
         //conn = DriverManager.getConnection(aa,bb,cc);

         out.println("<html>");
         out.println("<head><title>My Search Engine</title></head>");
         out.println("<body>");
         out.println("<h1>Search the Internet:</h1>");
         out.println("<form action=\"/SearchEngine/search\" method=\"get\">");
         out.println("<input name=\"query\" type=\"text\" size=\"80\">");
         out.println("<br><br>");
         out.println("<input type=\"submit\" value=\"Search\">");

         if(params.length != 0){
            String sqlQuery = "select * from words where word LIKE '"+params[0]+"'";
            
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sqlQuery);
            while(rs.next()){
               int urlID = rs.getInt("urlid");
               if(!vec.contains(urlID))
                  vec.add(urlID);
            }
            stmt.close();
            for(int i = 1; i < params.length; i++){
               //HashSet<Integer> tmp = new HashSet<Integer>();
               Vector<Integer> tmp = new Vector<Integer>(10);
               String sqlQuery2 = "select * from words where word LIKE '"+params[i]+"'";
               stmt = conn.createStatement();
               ResultSet rs2 = stmt.executeQuery(sqlQuery2);
               while(rs2.next()){
                  int urlID = rs2.getInt("urlid");
                  if(!tmp.contains(urlID) && vec.contains(urlID)){
                     tmp.add(urlID);
                  }
               }
               stmt.close();
               vec.retainAll(tmp);
            }
         }

         out.println("<hr>");

         out.println("<h2>Total "+vec.size()+" records found:</h2>");
         //Iterator it = hs.iterator();
         for(Integer urlID : vec){
            //int urlID = (int)it.next();
            String sqlQuery = "select * from urls where urlid = '"+urlID+"'";
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sqlQuery);
            if (rs.next()) {
               String url = rs.getString("url");
               String description = rs.getString("description");
               String imgSrc = rs.getString("image");
               
               out.println("<a href=\""+url+"\">");
               out.println("<img src=\""+imgSrc+"\" style=\"height: 50px; width:50px;\" border=\"3\">");
               out.println("</a>");
               
               out.println("<a href=\""+url+"\" font-size:200px>"+url+"</a>");
               out.println("<p>"+description+"</p>");
            }
            stmt.close();
         }
         out.println("</form>");
         out.println("</body>");
         out.println("</html>");
        
      }
      catch(Exception e){

      } finally {
         out.close();  // Always close the output writer
      }
   }
}