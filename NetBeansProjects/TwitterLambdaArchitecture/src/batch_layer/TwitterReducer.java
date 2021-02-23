/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package batch_layer;

import batch_layer.LingPipeTwitter;
import com.aliasi.classify.Classification;
import com.aliasi.classify.DynamicLMClassifier;
import com.aliasi.lm.NGramProcessLM;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author bernardo
 */
public class TwitterReducer extends Reducer<Text, Text, Text, FloatWritable> {
    private FloatWritable result = new FloatWritable();
    private HashMap<String, String> map = new HashMap<>();
    private Map <List<String>, Integer> queries;
    LingPipeTwitter pol;
    DynamicLMClassifier<NGramProcessLM> mClassifier;

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        
        HTable TableOut = new HTable(configuration, TableName.valueOf("SentimentAnalysisTweets"));
        Scan s = new Scan();
        System.out.println("Clean Up Reducer");
        s.setCaching(1);
        s.addColumn(Bytes.toBytes("Query"), Bytes.toBytes("QueryValue"));
        s.addColumn(Bytes.toBytes("Sentiment"), Bytes.toBytes("SentimentValue"));
        s.addColumn(Bytes.toBytes("TweetsData"), Bytes.toBytes("NumberTweets"));
        s.addColumn(Bytes.toBytes("TweetsData"), Bytes.toBytes("Percentual"));
        ResultScanner scanner = TableOut.getScanner(s);
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            byte[] value1 = result.getValue(Bytes.toBytes("Query"), Bytes.toBytes("QueryValue"));
            byte[] value2 = result.getValue(Bytes.toBytes("Sentiment"), Bytes.toBytes("SentimentValue"));
            System.out.println(Bytes.toString(value1));
            List <String> list = Collections.unmodifiableList(Arrays.asList(Bytes.toString(value1), Bytes.toString(value2)));
            System.out.println("queries.get(list) "+ queries.get(list));
            if(queries.containsKey(list)){
                Put p = new Put(Bytes.toBytes(Bytes.toString(result.getRow())));
                p.add(Bytes.toBytes("TweetsData"), Bytes.toBytes("NumberTweets"), Bytes.toBytes(String.valueOf(queries.get(list))));
                TableOut.put(p);
                System.out.println(Bytes.toString(value1));
            }
            System.out.println(Bytes.toString(result.getRow()));
            
        }
        //To change body of generated methods, choose Tools | Templates.
    }
    
    
    
    /*
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = HBaseConfiguration.create();
        Table table = new HTable(configuration, TableName.valueOf("TweetsAnalyzed"));
        Scan s = new Scan();
        s.setCaching(1);
        s.addColumn(Bytes.toBytes("Tweet"), Bytes.toBytes("Text"));
        ResultScanner scanner = table.getScanner(s);
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            byte[] value1 = result.getValue(Bytes.toBytes("Tweet"), Bytes.toBytes("Text"));
            map.put(Bytes.toString(value1), Bytes.toString(result.getRow()));
        }
    }
    
    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      Configuration configuration=HBaseConfiguration.create();
      Table table = new HTable(configuration, TableName.valueOf("TweetsAnalyzed"));
      String rowkey=map.get(key.toString());
      Put p = new Put(Bytes.toBytes(rowkey));
      p.add(Bytes.toBytes("Tweet"), Bytes.toBytes("Label"),Bytes.toBytes(result.toString()));
      table.put(p);
      context.write(key, result);
    }*/

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float positives,negatives,neutrals;
        int numVal;
        System.out.println("Lunghezza Lista tweets: "+map.size());
        //NLP.init();
        System.out.println("ciao scemo2"+key.toString());
        //System.out.println("ciao scemo2");
        positives=negatives=neutrals=numVal=0;
        Configuration configuration=context.getConfiguration();
        //HTable table = new HTable(configuration, TableName.valueOf("QueryTweetsAnalyzed"));
        HTable table = new HTable(configuration, TableName.valueOf("DatasetTweetsAnalyzed"));
        int count=0;
        for (Text val : values) {
            count+=1;
            System.out.println("ciao val:"+val.toString());
            int value=0;//NLP.findSentiment(val.toString());
            try{
            Classification classification=mClassifier.classify(val.toString());
            String s=classification.bestCategory();
            if(s.equals("4")){
                value=4;//NLP.findSentiment(tweet);
            }
            if(s.equals("2")){
                value=2;//NLP.findSentiment(tweet);
            }
            }catch (Exception ex) {
            }
            System.out.println(key.toString()+count);
            if(value<2){
                negatives+=1;
            }
            if(value>2){//Integer.parseInt(val.getLabel()
                positives+=1;
            }
            
            if(value==2){
                neutrals+=1;
            }
            numVal+=1;
            String rowkey = map.get(val.toString());
            Put p = new Put(Bytes.toBytes(rowkey));
            p.add(Bytes.toBytes("Tweet"), Bytes.toBytes("Label"), Bytes.toBytes(String.valueOf(value)));
            table.put(p);
            result.set(positives);
        }
        System.out.println("num tweets processed: " + numVal);
        queries.put(Collections.unmodifiableList(Arrays.asList(key.toString(),"Positive")), (int)positives);
        queries.put(Collections.unmodifiableList(Arrays.asList(key.toString(),"Negative")), (int)negatives);
        queries.put(Collections.unmodifiableList(Arrays.asList(key.toString(),"Neutral")), (int)neutrals);
        negatives/=numVal;
        positives/=numVal;
        neutrals/=numVal;
        negatives*=100;
        positives*=100;
        neutrals*=100;
        result.set(positives);
        context.write(key, result);
        result.set(neutrals);
        context.write(key, result);
        result.set(negatives);
        context.write(key, result);
        
        
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        queries = new HashMap<>();
        System.out.println("reduce");
        Configuration configuration = context.getConfiguration();
        System.out.println("reduce1");
        //HTable table = new HTable(configuration, TableName.valueOf("QueryTweetsAnalyzed"));
        HTable timetable = new HTable(configuration, TableName.valueOf("RowEndBatchCycle"));
        HTable table = new HTable(configuration, TableName.valueOf("DatasetTweetsAnalyzed"));
        
        Get g = new Get(Bytes.toBytes("row 0001"));
        Result r = timetable.get(g);
        String lastrow = Bytes.toString(r.getValue(Bytes.toBytes("Row"), Bytes.toBytes("RowValue")));
        System.out.println("last row Batch Cycle is: "+lastrow);
        Scan s = new Scan();
        System.out.println("reduce2");
        s.setCaching(1);
        s.setStopRow(Bytes.toBytes(lastrow));
        s.addColumn(Bytes.toBytes("Tweet"), Bytes.toBytes("Text"));
        ResultScanner scanner = table.getScanner(s);
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            byte[] value1 = result.getValue(Bytes.toBytes("Tweet"), Bytes.toBytes("Text"));
            map.put(Bytes.toString(value1), Bytes.toString(result.getRow()));
            System.out.println("b");
            System.out.println(Bytes.toString(result.getRow()));
            
        }
        System.out.println("reduce3");
        String [] args= new String[10];
        pol = new LingPipeTwitter(args);
        try{
            pol.train2();
            
        }catch(IOException e){
            
        }
        mClassifier=pol.getmClassifier2();
                System.out.println("reduce3");

    }
    

    
    
}