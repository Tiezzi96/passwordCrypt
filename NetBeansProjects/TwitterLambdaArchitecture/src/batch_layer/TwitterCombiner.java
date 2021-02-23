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
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 *
 * @author bernardo
 */
public class TwitterCombiner extends Reducer<CompositeGroupKey, IntWritable, Text, IntWritable> {
    
    private IntWritable result = new IntWritable();
    private HashMap<String, IntWritable> map = new HashMap<>();
    private Text tweet=new Text();

    
    @Override
    public void setup (Context context)throws IOException, InterruptedException, NumberFormatException{
        File file = new File("/home/bernardo/NetBeansProjects/Twitter4j/src/AFINN-111.txt");
        BufferedReader bufferedreader = new BufferedReader(new FileReader(file));
        String st;
        while ((st=bufferedreader.readLine()) != null) {            
             StringTokenizer itr = new StringTokenizer(st);
             String key="";
             while(itr.hasMoreTokens()){
                 if(key.isEmpty())
                    key=itr.nextToken();
                 String value = itr.nextToken();
                 try {
                     map.put(key, new IntWritable(Integer.parseInt(value)));
                 } catch (NumberFormatException e) {
                     key+=" " + value;
                 }
                 
             }
        }
    }
    @Override
    public void reduce(CompositeGroupKey key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      if(map.containsKey(key.getLabel())){
          sum *= (map.get(key.getLabel())).get();
      }else{
          sum*=Math.random()*100;
      }
      result.set(sum);
      tweet.set(key.getTweet());
        System.out.println(key.getTweet());
        System.out.println(sum);
      context.write(tweet, result);
    }
    
}