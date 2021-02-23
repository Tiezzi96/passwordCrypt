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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author bernardo
 */
public class TableHbaseMapper extends TableMapper<Text, Text> {
    private final static IntWritable one = new IntWritable(1);
    //private CompositeGroupKey tweetlabel = new CompositeGroupKey();
    private HashMap<String, IntWritable> map = new HashMap<>();
    private Text word = new Text();
    private IntWritable label = new IntWritable(1);
    public static final byte[] CF = "Tweet".getBytes();
    public static final byte[] ATTR1 = "Text".getBytes();
    public static final byte[] ATTR2 = "Query".getBytes();
    private Text query = new Text();
//    private Table table;
    private static final Logger logger = LogManager.getLogger(TableHbaseMapper.class);
    private static long timestamp;

    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
            super.setup(context); //To change body of generated methods, choose Tools | Templates.
            timestamp=new Timestamp(System.currentTimeMillis()).getTime();
    }

    /*
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
    }
    @Override
    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
    String val = new String(value.getValue(CF, ATTR1));
    StringTokenizer itr = new StringTokenizer(val);
    ArrayList<String> list = new ArrayList<>();
    while(itr.hasMoreTokens()){
    list.add(itr.nextToken());
    }
    int i=0;
    while (i < list.size()) {
    word.set(val);
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
    }*/
    public static void setTimestamp(long time) {
        timestamp=time;
    }

    public static boolean IsTweetAnalyzed(long time){
        return time>timestamp;
        
    } 
        
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        long time=value.rawCells()[0].getTimestamp();
        System.out.println("hello");
        System.out.println(timestamp);
        System.out.println(time);
        String q=Bytes.toString(value.getValue(CF, ATTR2));
        if (!q.equals("NO_QUERY")) {
            String val = new String(value.getValue(CF, ATTR1));
            System.out.println(val + " : " + time);
        //int l=NLP.findSentiment(val);
            //tweetlabel.set(val,String.valueOf(l));
            word.set(val);
        //System.out.println("ciao scemo: " +val);
            //logger.info("ciao scemo");
            query.set(q);
        //query.set("Ciao");
            System.out.println("Query: "+query.toString());
            context.write(query, word);

        }
    }
    
}