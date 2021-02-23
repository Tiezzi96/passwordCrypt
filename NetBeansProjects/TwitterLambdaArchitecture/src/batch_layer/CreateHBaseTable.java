/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package batch_layer;

/**
 *
 * @author bernardo
 */
import static batch_layer.PopulateHBaseTable.DB_URL;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.Connection;
//import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
//import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.HTable;

//import javax.jms.ConnectionFactory;
public class CreateHBaseTable {

    static final String DB_URL = "jdbc:derby://localhost:1527/MyDatabase";

    static final String USER = "Bernardo";
    static final String PASS = "bernardo";

    public static void createTableSentimentAnalysis() throws IOException {

        Configuration hconfig = HBaseConfiguration.create();
        hconfig.set("mapr.hbase.default.db", "hbase");
        //hconfig.addResource("/usr/local/Hbase/hbase-1.3.6/conf/hbase-site.xml");
        hconfig.set("hbase.zookeeper.property.clientPort", "2222");
        hconfig.set("hbase.zookeeper.quorum", "localhost");

        //hconfig.set("hbase.cluster.distributed", "true");
        //hconfig.set("hbase.rootdir", "hdfs://localhost:54310/hbase");
        HTableDescriptor htable = new HTableDescriptor(TableName.valueOf("SentimentAnalysisTweets"));
        htable.addFamily(new HColumnDescriptor("Query"));
        htable.addFamily(new HColumnDescriptor("Sentiment"));
        htable.addFamily(new HColumnDescriptor("TweetsData"));

        System.out.println("Connecting...");
        HBaseAdmin hbase_admin = new HBaseAdmin(hconfig);
        System.out.println("Creating Table...");
        hbase_admin.createTable(htable);
        System.out.println("Done!");
        // close HTable instance

        try {
            HTable table = new HTable(hconfig, "SentimentAnalysisTweets");
            System.out.println(table.getName());
            String[] queries = {"Trump", "Biden", "Covid"};
            String[] sentiments = {"Positive", "Negative", "Neutral"};
            int j = 0;
            int k = 0;
            for (int i = 1; i < 10; i++) {
                System.out.println("row 000" + i + " " + queries[k % 3] + " " + sentiments[j % 3]);

                Put p = new Put(Bytes.toBytes("row 000" + i));
                p.add(Bytes.toBytes("Query"), Bytes.toBytes("QueryValue"), Bytes.toBytes(queries[k % 3]));
                p.add(Bytes.toBytes("Sentiment"), Bytes.toBytes("SentimentValue"), Bytes.toBytes(sentiments[j % 3]));
                p.add(Bytes.toBytes("TweetsData"), Bytes.toBytes("NumberTweets"), Bytes.toBytes("0"));
                p.add(Bytes.toBytes("TweetsData"), Bytes.toBytes("Percentual"), Bytes.toBytes("0"));
                table.put(p);
                j += 1;
                if (j % 3 == 0) {
                    k += 1;
                }

            }
            /*
            Put p = new Put(Bytes.toBytes("row 0001"));
            p.add(Bytes.toBytes("TweetsData"),Bytes.toBytes("NumberTweets"),Bytes.toBytes(String.valueOf(0)));
            table.put(p);
            Get g = new Get(Bytes.toBytes("row 0001"));
            Result r = table.get(g);
            
            byte [] value = r.getValue(Bytes.toBytes("Query"),Bytes.toBytes("QueryValue"));
            
            byte [] value1 = r.getValue(Bytes.toBytes("Sentiment"),Bytes.toBytes("SentimentValue"));
            byte [] value2 = r.getValue(Bytes.toBytes("TweetsData"),Bytes.toBytes("NumberTweets"));
            
            String valueStr = Bytes.toString(value);
            String valueStr1 = Bytes.toString(value1);
            String valueStr2 = Bytes.toString(value2);
            System.out.println("GET: " +"Query: "+ valueStr+" SentimentValue: "+valueStr1+" NumberTweets:"+valueStr2);
            
             */

        } catch (Exception e) {
            return;
        }
    }

    public static void createQueryTweetsAnalyzed() throws IOException {
        // instantiate Configuration class

        Configuration hconfig = HBaseConfiguration.create();
        hconfig.set("mapr.hbase.default.db", "hbase");
        //hconfig.addResource("/usr/local/Hbase/hbase-1.3.6/conf/hbase-site.xml");
        hconfig.set("hbase.zookeeper.property.clientPort", "2222");
        hconfig.set("hbase.zookeeper.quorum", "localhost");

        //hconfig.set("hbase.cluster.distributed", "true");
        //hconfig.set("hbase.rootdir", "hdfs://localhost:54310/hbase");
        HTableDescriptor htable = new HTableDescriptor(TableName.valueOf("QueryTweetsAnalyzed"));
        htable.addFamily(new HColumnDescriptor("Id"));
        htable.addFamily(new HColumnDescriptor("Date"));
        htable.addFamily(new HColumnDescriptor("Tweet"));

        System.out.println("Connecting...");
        HBaseAdmin hbase_admin = new HBaseAdmin(hconfig);
        System.out.println("Creating Table...");
        hbase_admin.createTable(htable);
        System.out.println("Done!");
        // close HTable instance

        /*
            Scan s = new Scan();
            s.addColumn(Bytes.toBytes("Id"), Bytes.toBytes("col1"));
            s.addColumn(Bytes.toBytes("Date"), Bytes.toBytes("col2"));
            ResultScanner scanner = table.getScanner(s);
            for (Result rnext = scanner.next(); rnext != null; rnext = scanner.next())
            {
                System.out.println("Found row : " + rnext);
            }
            scanner.close();
         */
    }

    public static void createDatasetTweetsAnalyzed() throws IOException {
        // instantiate Configuration class

        Configuration hconfig = HBaseConfiguration.create();
        hconfig.set("mapr.hbase.default.db", "hbase");
        //hconfig.addResource("/usr/local/Hbase/hbase-1.3.6/conf/hbase-site.xml");
        hconfig.set("hbase.zookeeper.property.clientPort", "2222");
        hconfig.set("hbase.zookeeper.quorum", "localhost");

        //hconfig.set("hbase.cluster.distributed", "true");
        //hconfig.set("hbase.rootdir", "hdfs://localhost:54310/hbase");
        HTableDescriptor htable = new HTableDescriptor(TableName.valueOf("DatasetTweetsAnalyzed"));
        htable.addFamily(new HColumnDescriptor("Id"));
        htable.addFamily(new HColumnDescriptor("Date"));
        htable.addFamily(new HColumnDescriptor("Tweet"));

        System.out.println("Connecting...");
        HBaseAdmin hbase_admin = new HBaseAdmin(hconfig);
        System.out.println("Creating Table...");
        hbase_admin.createTable( htable );
        System.out.println("Done!");
        // close HTable instance
        HTable table = new HTable(hconfig, "DatasetTweetsAnalyzed");
        Connection conn = null;
        Statement stmt = null;
        int i = 102;
        DecimalFormat decimalFormat = new DecimalFormat("00000000");
        System.out.println(decimalFormat.format(i));
        try {
            //STEP 2: Register JDBC driver
            //Class.forName("com.mysql.jdbc.Driver");

            //STEP 3: Open a connection
            System.out.println("Connecting to a selected database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            System.out.println("Connected database successfully...");

            //STEP 4: Execute a query
            System.out.println("Creating table in given database...");
            stmt = conn.createStatement();;
            String sql = "SELECT * FROM TWEETSTABLE2 where TARGET='0' AND ROWNUM>=100000 AND ROWNUM<150000";
            System.out.println(sql);
            ResultSet rs = stmt.executeQuery(sql);
            int id = 0;
            while (rs.next()) {
                id+=1;
                String ID = decimalFormat.format(id);
                Put p = new Put(Bytes.toBytes("row"+ID));
                p.add(Bytes.toBytes("Id"), Bytes.toBytes("AccountID"), Bytes.toBytes(rs.getString("ID")));
                p.add(Bytes.toBytes("Date"), Bytes.toBytes("CreationDate"), Bytes.toBytes(rs.getString("DATE")));
                p.add(Bytes.toBytes("Tweet"), Bytes.toBytes("Query"), Bytes.toBytes(rs.getString("FLAG")));
                p.add(Bytes.toBytes("Tweet"), Bytes.toBytes("Text"), Bytes.toBytes(rs.getString("TEXT")));
                p.add(Bytes.toBytes("Tweet"), Bytes.toBytes("Label"), Bytes.toBytes(rs.getString("TARGET")));
                table.put(p);
            }

            System.out.println("Insert table in given database...");
            
            sql = "SELECT * FROM TWEETSTABLE2 where TARGET='4' AND ROWNUM>=900000 AND ROWNUM<950000";
            rs = stmt.executeQuery(sql);
            id = 50000;
            while (rs.next()) {
                id+=1;
                String ID = decimalFormat.format(id);
                Put p = new Put(Bytes.toBytes("row"+ID));
                p.add(Bytes.toBytes("Id"), Bytes.toBytes("AccountID"), Bytes.toBytes(rs.getString("ID")));
                p.add(Bytes.toBytes("Date"), Bytes.toBytes("CreationDate"), Bytes.toBytes(rs.getString("DATE")));
                p.add(Bytes.toBytes("Tweet"), Bytes.toBytes("Query"), Bytes.toBytes(rs.getString("FLAG")));
                p.add(Bytes.toBytes("Tweet"), Bytes.toBytes("Text"), Bytes.toBytes(rs.getString("TEXT")));
                p.add(Bytes.toBytes("Tweet"), Bytes.toBytes("Label"), Bytes.toBytes(rs.getString("TARGET")));
                table.put(p);
            }

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
        }//end finally try

        Scan s = new Scan();
        s.setCaching(500);
        s.addColumn(Bytes.toBytes("Tweet"), Bytes.toBytes("Query"));
        s.addColumn(Bytes.toBytes("Tweet"), Bytes.toBytes("Text"));
        ResultScanner scanner = table.getScanner(s);
        String[] query = {"Trump", "Biden", "Covid"};
        for (Result rnext = scanner.next(); rnext != null; rnext = scanner.next()) {
            System.out.println("Row : " + Bytes.toString(rnext.getRow())
                    + " Query is:"+Bytes.toString(rnext.getValue(Bytes.toBytes("Tweet"), Bytes.toBytes("Query")))
                            + " Query is:"+Bytes.toString(rnext.getValue(Bytes.toBytes("Tweet"), Bytes.toBytes("Text"))));
            
            byte[] v = rnext.getValue(Bytes.toBytes("Tweet"), Bytes.toBytes("Text"));
            String t = Bytes.toString(v);
            String queryString = "";
            for (String string : query) {
                for (String word : t.split(" ")) {
                    if ((word.toLowerCase(Locale.ITALY).contains(string.toLowerCase()) && word.contains("#"))
                            || ((word.toLowerCase(Locale.ITALY).contains(string.toLowerCase()) && word.contains("@")))) {
                        queryString = string;
                    } else if ((word.toLowerCase(Locale.ITALY).contains(string.toLowerCase()))) {
                        queryString = string;
                    }
                }
                if (!queryString.equals("")) {
                    System.out.println("Found row : " + Bytes.toString(rnext.getRow()));
                    Put p = new Put(rnext.getRow());
                    p.add(Bytes.toBytes("Tweet"), Bytes.toBytes("Query"), Bytes.toBytes(queryString));
                    table.put(p);
                }
            }
        }
        scanner.close();

    }

    public static void createRowEndBatchCycle() {
        try {
            Configuration hconfig = HBaseConfiguration.create();
            hconfig.set("mapr.hbase.default.db", "hbase");
            //hconfig.addResource("/usr/local/Hbase/hbase-1.3.6/conf/hbase-site.xml");
            hconfig.set("hbase.zookeeper.property.clientPort", "2222");
            hconfig.set("hbase.zookeeper.quorum", "localhost");

            //hconfig.set("hbase.cluster.distributed", "true");
            //hconfig.set("hbase.rootdir", "hdfs://localhost:54310/hbase");
            HTableDescriptor htable = new HTableDescriptor(TableName.valueOf("RowEndBatchCycle"));
            htable.addFamily(new HColumnDescriptor("Time"));
            htable.addFamily(new HColumnDescriptor("Row"));

            System.out.println("Connecting...");
            HBaseAdmin hbase_admin = new HBaseAdmin(hconfig);
            System.out.println("Creating Table...");
            //hbase_admin.createTable(htable);
            System.out.println("Done!");
            HTable table = new HTable(hconfig, "RowEndBatchCycle");
            Put p = new Put(Bytes.toBytes("row 0001"));
            p.add(Bytes.toBytes("Time"), Bytes.toBytes("TimeValue"), Bytes.toBytes(String.valueOf(0)));
            p.add(Bytes.toBytes("Row"), Bytes.toBytes("RowValue"), Bytes.toBytes(String.valueOf("row00100001")));

            table.put(p);
        } catch (Exception e) {
            System.out.println("Exception is occurred: ");
            e.printStackTrace();
        }

    }

    public static void createLastRowTimeStamp() throws Exception {
        Configuration hconfig = HBaseConfiguration.create();
        hconfig.set("mapr.hbase.default.db", "hbase");
        //hconfig.addResource("/usr/local/Hbase/hbase-1.3.6/conf/hbase-site.xml");
        hconfig.set("hbase.zookeeper.property.clientPort", "2222");
        hconfig.set("hbase.zookeeper.quorum", "localhost");

        //hconfig.set("hbase.cluster.distributed", "true");
        //hconfig.set("hbase.rootdir", "hdfs://localhost:54310/hbase");
        HTableDescriptor htable = new HTableDescriptor(TableName.valueOf("LastRowTimeStamp"));
        htable.addFamily(new HColumnDescriptor("Time"));
        htable.addFamily(new HColumnDescriptor("Row"));

        System.out.println("Connecting...");
        HBaseAdmin hbase_admin = new HBaseAdmin(hconfig);
        System.out.println("Creating Table...");
        hbase_admin.createTable(htable);
        System.out.println("Done!");

        HTable table = new HTable(hconfig, "RowEndBatchCycle");
        Put p = new Put(Bytes.toBytes("row 0001"));
        p.add(Bytes.toBytes("Time"), Bytes.toBytes("TimeValue"), Bytes.toBytes(String.valueOf(0)));
        p.add(Bytes.toBytes("Row"), Bytes.toBytes("RowValue"), Bytes.toBytes(String.valueOf(0)));
        table.put(p);
    }

    public static void main(String[] args) throws IOException {
        CreateHBaseTable.createDatasetTweetsAnalyzed();
        try{
            CreateHBaseTable.createLastRowTimeStamp();
        
        }catch(Exception e){
            
        }
        String[] query = {"Covid", "Trump", "Biden"};
        String s = " I’m not anti vaccines, but I won’t be taking the COVID Jab and neither will members of my family, and a large percentage";
        String queryString = "";
        if (!s.equals("")) {
            for (String i : query) {
                System.out.println(i);
                for (String word : s.split(" ")) {
                    if ((word.toLowerCase(Locale.ITALY).contains(i.toLowerCase()) && word.contains("#"))
                            || ((word.toLowerCase(Locale.ITALY).contains(i.toLowerCase()) && word.contains("@")))) {
                        queryString = i;
                        break;
                    } else if ((word.toLowerCase(Locale.ITALY).contains(i.toLowerCase()))) {
                        queryString = i;
                        break;
                    }
                }
            }
            if (queryString.equals("")) {
                queryString = "NO QUERY";
            }

            System.out.println("Query is:" + queryString);
        }

    }
}
