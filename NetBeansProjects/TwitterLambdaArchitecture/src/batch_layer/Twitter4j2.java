/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package batch_layer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import twitter4j.Paging;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 *
 * @author bernardo
 */
public class Twitter4j2 {

    /**
     * @param args the command line arguments
     */
    static final String DB_URL = "jdbc:derby://localhost:1527/MyDatabase";

    //  Database credentials
    static final String USER = "Bernardo";
    static final String PASS = "bernardo";
    
    public static void main(String[] args) throws TwitterException {
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setDebugEnabled(true)
                .setOAuthConsumerKey("YZcQGYa54T8Ueqg5tiXZjM3HZ")
                .setOAuthConsumerSecret("sDkH58gAGC9ePR0TTfVpKKH9i18aW84McUOUpvldpBPdqPmKvc")
                .setOAuthAccessToken("1284233485859860480-LvKw4EvYhwKVfqopmKCnc1bGAAeZIC")
                .setOAuthAccessTokenSecret("XG7uPiIt0VHnK5FCnAQK1rrFPWmzZSWrni8XdfnLjpOL0");

        TwitterFactory tf = new TwitterFactory(configurationBuilder.build());
        twitter4j.Twitter twitter = tf.getInstance();
        Paging page = new Paging(1, 200);
        String[] users = {"@HRCSaudi_EN", "@amazon", "@awscloud", "@nytimes", "@Microsoft", "@Huawei", "@Twitter", "@WSJ", "@MetroUK", "@BarackObama", "@JoeBiden"};
        String[] hashtags = {"#COVID19", "#BlackLivesMatter", "#JoeBiden2020", "#Israel", "#5G", "#BlackFriday"};
        /*
        Query query = new Query("#5G");
        query.setCount(100);
        query.setLang("en");

        int searchResultCount;
        long lowestTweetId = Long.MAX_VALUE;
        int sizetotal=0;
        ArrayList<String> tweets = new ArrayList<>();
        do {
            QueryResult queryResult = twitter.search(query);

            searchResultCount = queryResult.getTweets().size();
            sizetotal += queryResult.getTweets().size();
            System.out.println("Query result size "+queryResult.getTweets().size());
            System.out.println("sizetotal "+sizetotal);
            for (Status tweet : queryResult.getTweets()) {

                // do whatever with the tweet

                if (tweet.getId() < lowestTweetId) {
                    lowestTweetId = tweet.getId();
                    query.setMaxId(lowestTweetId);
                if(!tweets.contains(tweet.getText()))
                    tweets.add(tweet.getText());
                }
            }
            

        } while (searchResultCount != 0 && searchResultCount % 100 == 0);
        System.out.println(tweets.size());
        */
        
        for (String it : hashtags) {
            //List<Status> status = twitter.getUserTimeline(it, page);
            //Query q=new Query("@HRCSaudi_EN");
            //q.setLang("en");
            //q.count(100);
            //List<Status> status =twitter.search(q).getTweets();
            
        Query query = new Query(it);
        query.setCount(100);
        query.setLang("en");

        int searchResultCount;
        long lowestTweetId = Long.MAX_VALUE;
        int sizetotal=0;
        ArrayList<String> tweets = new ArrayList<>();
        ArrayList<Status> statuslist = new ArrayList<>();
        do {
            QueryResult queryResult = twitter.search(query);

            searchResultCount = queryResult.getTweets().size();
            sizetotal += queryResult.getTweets().size();
            System.out.println("Query result size "+queryResult.getTweets().size());
            System.out.println("sizetotal "+sizetotal);
            for (Status tweet : queryResult.getTweets()) {

                // do whatever with the tweet

                if (tweet.getId() < lowestTweetId) {
                    lowestTweetId = tweet.getId();
                    query.setMaxId(lowestTweetId);
                if(!tweets.contains(tweet.getText())){
                    tweets.add(tweet.getText());
                    statuslist.add(tweet);
                }   
                }
            }
            

        } while (searchResultCount != 0 && searchResultCount % 100 == 0);
        System.out.println(tweets.size());
        /*

            System.out.println(status.size());
            List<Status> removed = new ArrayList<>();
            for (Status s : status) {
                String withoutAccent = Normalizer.normalize(s.getText(), Normalizer.Form.NFD);
                String output = withoutAccent.replaceAll("[^a-zA-Z ]", "").trim();
                if (output.isEmpty()) {
                    removed.add(s);
                }
            }
            for (Status r : removed) {
                status.remove(r);
            }
            for (Status s : status) {
                Pattern pattern = Pattern.compile("([^0-9A-Za-z \\t])|(\\w+:\\/\\/\\S+)");
                Matcher matcher = pattern.matcher(s.getText());
                String textWithoutHashtagsAndUrls = matcher.replaceAll("");
                System.out.println(s.getUser().getName() + "      " + textWithoutHashtagsAndUrls + "      " + s.getId() + "      " + s.getCreatedAt());
            }
        

            System.out.println(status.size());
        */
        Connection conn = null;
            Statement stmt = null;
            try {
                //STEP 2: Register JDBC driver
                //Class.forName("com.mysql.jdbc.Driver");

                //STEP 3: Open a connection
                System.out.println("Connecting to a selected database...");
                conn = DriverManager.getConnection(DB_URL, USER, PASS);
                System.out.println("Connected database successfully...");

                //STEP 4: Execute a query
                System.out.println("Creating table in given database...");
                stmt = conn.createStatement();

                int count = 0;
                for (Status s : statuslist) {
                    //System.out.println(s.getUser().getName() + "      " + s.getText() + "      " + s.getId()+ "      " + s.getCreatedAt());            
                    Pattern pattern = Pattern.compile("([^0-9A-Za-z \\t])|(\\w+:\\/\\/\\S+)");
                    Matcher matcher = pattern.matcher(s.getText());
                    String text = matcher.replaceAll("");
                    Timestamp time = new Timestamp(s.getCreatedAt().getTime());
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    if (count == 23) {
                        System.out.println(s.getUser().getScreenName());
                        System.out.println(time);
                        System.out.println(s.getText());
                    }
                    String tmp = "'";
                    char tm = tmp.charAt(0);
                    int i = 0;
                    while (i < text.length()) {
                        char c = text.charAt(i);
                        if (tm == c) {
                            String str2 = text.substring(i);
                            //System.out.println("str2"+str2);
                            String str1 = text.substring(0, i);
                            //System.out.println("str1"+str1);
                            //System.out.println(text);
                            text = str1 + "'" + str2;
                            i += 1;

                        }

                        i += 1;

                    }

                    System.out.println(text);
                    String t = formatter.format(time);
                    String sql = "INSERT INTO QUERYTWEETS2 (ACCOUNTID,DATE,QUERY,TEXT,LABEL) "
                            + "VALUES('" + s.getUser().getScreenName() + "','" + time + "','"+it+"','" + text + "',0" + ")";
                    System.out.println(sql);
                    stmt.executeUpdate(sql);
                    count += 1;

                }

                //String sql2="DELETE FROM TWEETS WHERE 1=1";
                //stmt.executeUpdate(sql2);
               /*
           String sql = "CREATE TABLE Tweets"+
                   "(ID INT not NULL PRIMARY KEY, "+
                   "AccountID VARCHAR(255),"+
                   "Date TIMESTAMP,"+
                   "Query VARCHAR(255),"+
                   "Text VARCHAR(511))";
                 */
               System.out.println("Insert table in given database...");

            } catch (SQLException se) {
                //Handle errors for JDBC
                se.printStackTrace();
            } catch (Exception e) {
                //Handle errors for Class.forName
                e.printStackTrace();
            } finally {
                //finally block used to close resources
                try {
                    if (stmt != null) {
                        conn.close();
                    }
                } catch (SQLException se) {
                }// do nothing
                try {
                    if (conn != null) {
                        conn.close();
                    }
                } catch (SQLException se) {
                    se.printStackTrace();
                }//end finally try
            }//end try
            System.out.println("Goodbye!");

            // TODO code application logic here
        }

    }
    
}






