/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package batch_layer;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import static batch_layer.TwitterLambdaArchitecture.DB_URL;
import twitter4j.Status;
/**
 *
 * @author bernardo
 */
public class TwitterMapper extends Mapper<Object,Text,Text,IntWritable> {
    
    private final static IntWritable one = new IntWritable(1);
    //private CompositeGroupKey word = new CompositeGroupKey();
    private HashMap<String, IntWritable> map = new HashMap<>();
    private Text word = new Text();
    private IntWritable label = new IntWritable(1);
    static final String DB_URL = "jdbc:derby://localhost:1527/MyDatabase";
    static final String USER = "Bernardo";
    static final String PASS = "bernardo";

    /**
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     * @throws NumberFormatException
     */
    @Override
    public void setup(Context context) throws IOException, InterruptedException, NumberFormatException {
        File file = new File("/home/bernardo/NetBeansProjects/Twitter4j/src/AFINN-111.txt");
        BufferedReader bufferedreader = new BufferedReader(new FileReader(file));
        String st;
        while ((st = bufferedreader.readLine()) != null) {
            StringTokenizer itr = new StringTokenizer(st);
            String key = "";
            while (itr.hasMoreTokens()) {
                if (key.isEmpty()) {
                    key = itr.nextToken();
                }
                String value = itr.nextToken();
                try {
                    map.put(key, new IntWritable(Integer.parseInt(value)));
                } catch (NumberFormatException e) {
                    key += " " + value;
                }

            }
        }
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
            stmt = conn.createStatement();;
            String sql = "SELECT ID,TEXT FROM TWEETS";
            System.out.println(sql);
            ResultSet rs=stmt.executeQuery(sql);
            ArrayList<String> dataset = new ArrayList<>();
            while ( rs.next()) {
                dataset.add(rs.getString("TEXT"));
            }

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
        }//end finally try
    }
    

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        ArrayList<String> list = new ArrayList<>();
        while(itr.hasMoreTokens()){
            list.add(itr.nextToken());
        }
        int i=0;
        while (i < list.size()) {
            word.set(value.toString());
            String token = list.get(i);
            String token2 = "";
            String token3 = "";
            if ((i+1)<list.size()) {
                token2 = list.get(i + 1);
            }
            if ((i+2)<list.size()) {
                token3 = list.get(i + 2);
            }
            if (map.containsKey(token + " " + token2 + " " + token3) && !token2.isEmpty() && !token3.isEmpty()) {
                token += " " + token2 + " " + token3;
                label.set(one.get() * map.get(token).get());
                i += 3;
            } else if (map.containsKey(token + " " + token2) && !token2.isEmpty()) {
                token += " " + token2;
                label.set(one.get() * map.get(token).get());
                i += 2;
            } else {
                System.out.println(value.toString());
                System.out.println(token);
                if (map.containsKey(token)) {
                    label.set(one.get() * map.get(token).get());
                } else {
                    label.set(0);
                }
                i++;
            }
            context.write(word, label);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context); //To change body of generated methods, choose Tools | Templates.
    }
    
    
}
