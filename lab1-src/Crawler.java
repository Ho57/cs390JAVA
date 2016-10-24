import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.io.*;
import java.net.*;
import java.util.regex.*;
import java.sql.*;
import java.util.*;

public class Crawler
{
	Connection connection;
	int urlID;
	public Properties props;
	static Queue<String> toVisit;
	static int count;

	Crawler() {
		urlID = 0;
		count = 0;
		toVisit = new LinkedList<String>();
	}

	public void readProperties() throws IOException {
      		props = new Properties();
      		FileInputStream in = new FileInputStream("database.properties");
      		props.load(in);
      		in.close();
	}

	public void openConnection() throws SQLException, IOException
	{
		String drivers = props.getProperty("jdbc.drivers");
      		if (drivers != null) System.setProperty("jdbc.drivers", drivers);

      		String url = props.getProperty("jdbc.url");
      		String username = props.getProperty("jdbc.username");
      		String password = props.getProperty("jdbc.password");

		connection = DriverManager.getConnection( url, username, password);
   	}

	public void createDB() throws SQLException, IOException {
		openConnection();

         	Statement stat = connection.createStatement();
		
		// Delete the table first if any
		try {
			stat.executeUpdate("DROP TABLE urls");
			stat.executeUpdate("DROP TABLE words");
		}
		catch (Exception e) {
		}
			
		// Create the table
        	stat.executeUpdate("CREATE TABLE urls (urlid INT, url VARCHAR(512), description VARCHAR(200))");
        	stat.executeUpdate("CREATE TABLE words (word VARCHAR(50), urlid INT)");
	}

	public boolean urlInDB(String urlFound) throws SQLException, IOException {
        Statement stat = connection.createStatement();
		ResultSet result = stat.executeQuery( "SELECT * FROM urls WHERE url LIKE '"+urlFound+"'");

		if (result.next()) {
	        	System.out.println("URL "+urlFound+" already in DB");
			return true;
		}
	       // System.out.println("URL "+urlFound+" not yet in DB");
		return false;
	}

	public boolean wordInUrlInDB(String word, int urlID) throws SQLException, IOException {
        Statement stat = connection.createStatement();
		ResultSet result = stat.executeQuery( "SELECT * FROM words WHERE word LIKE '"+word+"' AND urlid LIKE '"+urlID+"'");

		if (result.next()) {
	        	System.out.println("word "+word+" with urlid "+urlID+" already in DB");
			return true;
		}
	       // System.out.println("URL "+urlFound+" not yet in DB");
		return false;
	}

	public void insertURLInDB( String url, String description) throws SQLException, IOException {
        Statement stat = connection.createStatement();
		String query = "INSERT INTO urls VALUES ('"+urlID+"','"+url+"','"+description+"')";
		//System.out.println("Executing "+query);
		stat.executeUpdate( query );
		urlID++;
	}

	public void insertWordInDB(String word, int urlID) throws SQLException, IOException{
        Statement stat = connection.createStatement();
		String query = "INSERT INTO urls VALUES ('"+word+"','"+urlID+"')";
		//System.out.println("Executing "+query);
		stat.executeUpdate( query );
	}

/*
	public String makeAbsoluteURL(String url, String parentURL) {
		if (url.indexOf(":")<0) {
			// the protocol part is already there.
			return url;
		}

		if (url.length > 0 && url.charAt(0) == '/') {
			// It starts with '/'. Add only host part.
			int posHost = url.indexOf("://");
			if (posHost <0) {
				return url;
			}
			int posAfterHist = url.indexOf("/", posHost+3);
			if (posAfterHist < 0) {
				posAfterHist = url.Length();
			}
			String hostPart = url.substring(0, posAfterHost);
			return hostPart + "/" + url;
		} 

		// URL start with a char different than "/"
		int pos = parentURL.lastIndexOf("/");
		int posHost = parentURL.indexOf("://");
		if (posHost <0) {
			return url;
		}
	}
*/

   	public void fetchURL(String url) {
        // process url
        if(url.startsWith("www") == true)
            url = "http://" + url;
        if(url.endsWith("/"))
            url = url.substring(0, url.length()-1);

        // check for invalid link
        if (url.contains("@")
                || url.contains(":80")
                || url.contains(".pdf")
                || url.contains(".pptx")
                || url.contains(".jpg"))
            return;
		try{
			// Check if it is already in the database
			if (urlInDB(url))
				return;

			System.out.println("Inserted URL: " + url + " into database");
			// Crawl page with jsoup
	        Document doc = null;
	    
            doc = Jsoup.connect(url)
                    .userAgent("Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:25.0) Gecko/20100101 Firefox/25.0")
                    .referrer("http://www.google.com")
                    .timeout(10000)
                    .ignoreHttpErrors(true)
                    .get();

			//Text processing
	        String text = doc.body().text();
	        StringBuilder sb = new StringBuilder();
	        for(int i = 0; i< text.length(); i++) {
	        	if(sb.length() >= 100)
	        		break;
	            char c = text.charAt(i);
	            if (Character.isAlphabetic(c) || Character.isDigit(c) || Character.isWhitespace(c)) {
	                sb.append(c);
	            }
	        }
	        String description = sb.toString();
	        // Insert url and description into database
			insertURLInDB(url, description);
			count++;

	        Elements questions = doc.select("a[href]");
	        for (Element link : questions) {
	            String adjacent_link = link.attr("abs:href");
	            toVisit.add(adjacent_link);
	        }
    	}
        catch(Exception e){
        	e.printStackTrace();
        }

	}

   	public static void main(String[] args)
   	{
		Crawler crawler = new Crawler();

		try {
			crawler.readProperties();
			String root = crawler.props.getProperty("crawler.root");
			crawler.createDB();
			toVisit.add(root);
		}
		catch( Exception e) {
         		e.printStackTrace();
		}
		while(count < 1000 && !toVisit.isEmpty()){
			String url = toVisit.poll();
			crawler.fetchURL(url);
		}
	}
}

