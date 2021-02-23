/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package batch_layer;

import speed_layer.TwitterTopology;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.utils.Utils;

/**
 *
 * @author bernardo
 */
public class TwitterJob {

    /*
    public static class TwitterMapper2 extends Mapper<Object,Text,Text,IntWritable> {
    
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        @Override
        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
        @Override
        protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
            super.cleanup(context); //To change body of generated methods, choose Tools | Templates.
        }
    
    
    }
    
    public static class TwitterReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
    
        private IntWritable result = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
        }
    
    }    */
    public static void main(String[] args) throws Exception {
        System.out.println("ciao");
        Configuration config = new Configuration();
        //NLP.init();
        //Configuration config = HBaseConfiguration.create();

        config.set("fs.default.name", "hdfs://localhost:54310");
        config.set("mapred.job.tracker", "http://localhost:54311");
        config.set("mapreduce.framework.name", "yarn");
        config.set("mapreduce.job.reduces", "1");
        config.set("mapreduce.reduce.speculative", "true");
        config.set("mapreduce.tasktracker.reduce.tasks.maximum", "8");
        config.set("hbase.zookeeper.property.clientPort", "2222");
        config.set("hbase.cluster.distributed", "true");
        config.set("hbase.rootdir", "hdfs://localhost:54310/hbase");
        config.set("mapreduce.task.timeout", "6000000");

        //config.set("hadoop.registry.zk.quorum", "localhost:2222");
        //config.set("hbase.zookeeper.property.maxClientCnxns","20000");
        //config.setInt("mapreduce.job.reduces", 6);
        //config.set("mapred.reduce.tasks.speculative.execution", "true");
        //config.set("mapred.reduce.tasks", "6");
        //config.set("ha.zookeeper.quorum", "127.0.0.1:2222");
        //config.set("yarn.resourcemanager.zk-address", "127.0.0.1:2222");
        //config.set("TwitterLambdaArchitecture.jar","/home/bernardo/NetBeansProjects/TwitterLambdaArchitecture/dist/TwitterLambdaArchitecture.jar");
        //config.setInt("mapreduce.tasktracker.reduce.tasks.maximum", 8);
        //config.setInt("mapred.tasktracker.reduce.tasks.maximum", 7);
        int c = 48;
        long InitExecution = System.currentTimeMillis();
        Thread.sleep(3000);
        
        long EndBatchCycle = System.currentTimeMillis();
        System.out.println(EndBatchCycle - InitExecution);
        
        while ((EndBatchCycle - InitExecution) < 300000) {
            Job job = Job.getInstance(config, "cirillo");
            job.setJarByClass(TwitterJob.class);
            //Configuration config = HBaseConfiguration.create();

            //va decommentato per la distributed mode
            job.setJar("/home/bernardo/NetBeansProjects/TwitterLambdaArchitecture/dist/AllInOneJar.jar");
            //job.setJar("/home/bernardo/Scaricati/stanford-corenlp-latest/stanford-corenlp-4.1.0/stanford-corenlp-4.1.0.jar");
            //HTable table = new HTable(config, TableName.valueOf("QueryTweetsAnalyzed"));
            HTable table = new HTable(config, TableName.valueOf("DatasetTweetsAnalyzed"));
            Scan s = new Scan();
            s.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
            s.setCacheBlocks(false);// don't set to true for MR jobs
            /*
             s.addFamily(Bytes.toBytes("Id"));
             s.addFamily(Bytes.toBytes("Tweet"));
             s.setStartRow(Bytes.toBytes("row1"));
             s.setStopRow(Bytes.toBytes("row99"));
             s.addColumn(Bytes.toBytes("Id"), Bytes.toBytes("AccountID"));
             s.addColumn(Bytes.toBytes("Tweet"), Bytes.toBytes("Text"));
             //s.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("TWEETS"));
             */
// set other scan attrs

            //TableHbaseMapper.setTimestamp(new Timestamp(System.currentTimeMillis()).getTime());
            //System.out.println("timestamp: "+new Timestamp(System.currentTimeMillis()).getTime());
            //job.setMapperClass(TwitterMapper.class);
            //job.setMapperClass(TableHbaseMapper.class);
            //job.setMapOutputKeyClass(Text.class);
            //job.setMapOutputValueClass(IntWritable.class);
            /*
            HTable timetable= new HTable(config, TableName.valueOf("LastRowTimeStamp"));
        Get g =  new Get(Bytes.toBytes("row 0001"));
        Result r = timetable.get(g);
        String TimeStamp = Bytes.toString(r.getValue(Bytes.toBytes("Time"),Bytes.toBytes("TimeValue")));
        System.out.println("TimeStamp Value is "+TimeStamp);
        Scan s1 = new Scan();
        s1.setCaching(1);
        s1.addColumn(Bytes.toBytes("Id"), Bytes.toBytes("AccountID"));
        s1.addColumn(Bytes.toBytes("Tweet"), Bytes.toBytes("Text"));
        s1.addColumn(Bytes.toBytes("Tweet"), Bytes.toBytes("Label"));
        ResultScanner scanner = table.getScanner(s1);
        String row="";
        try {
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                System.out.println("TimeStamp is " + String.valueOf(result.rawCells()[0].getTimestamp()));
                if ((String.valueOf(result.rawCells()[0].getTimestamp()).equals(TimeStamp))) {
                    byte[] value = result.getValue(Bytes.toBytes("Id"), Bytes.toBytes("AccountID"));
                    byte[] value1 = result.getValue(Bytes.toBytes("Tweet"), Bytes.toBytes("Text"));
                    byte[] value2 = result.getValue(Bytes.toBytes("Tweet"), Bytes.toBytes("Label"));
                    byte[] value3 = result.getValue(Bytes.toBytes("Tweet"), Bytes.toBytes("Query"));

                    String valueStr = Bytes.toString(value);
                    String valueStr1 = Bytes.toString(value1);
                    String valueStr2 = Bytes.toString(value2);
                    String valueStr3 = Bytes.toString(value3);
                    row = Bytes.toString(result.getRow());
                    System.out.println("Row " + row + " Id: " + valueStr + " Text: " + valueStr1 + " Query: " + valueStr3 + " Label: " + valueStr2);
                }
            }
            scanner.close();
            //}
        } catch (Exception ex) {

        }
        
            System.out.println("lastrow is "+row);
             */
            HTable timetable = new HTable(config, TableName.valueOf("LastRowTimeStamp"));
            Get g = new Get(Bytes.toBytes("row 0001"));
            Result r = timetable.get(g);
            String lastrow = Bytes.toString(r.getValue(Bytes.toBytes("Row"), Bytes.toBytes("RowValue")));
            System.out.println("Row Value is " + lastrow);
            
            timetable = new HTable(config, TableName.valueOf("RowEndBatchCycle"));
            Put p = new Put(Bytes.toBytes("row 0001"));
            p.add(Bytes.toBytes("Row"), Bytes.toBytes("RowValue"), Bytes.toBytes(lastrow));
            timetable.put(p);
            
            s.setStartRow(Bytes.toBytes("row00000001"));
            s.setStopRow(Bytes.toBytes(lastrow));
            //s.setStopRow(Bytes.toBytes("row00100001"));
            //TableInputFormat.configureSplitTable(job, table.getName());
            //TableInputFormat.addColumns(s, s.getFamilies());
            TableMapReduceUtil.initTableMapperJob(
                    //Bytes.toBytes("QueryTweetsAnalyzed"), // input table
                    Bytes.toBytes("DatasetTweetsAnalyzed"),
                    s, // Scan instance to control CF and attribute selection
                    TableHbaseMapper.class, // mapper class
                    Text.class, // mapper output key
                    Text.class, // mapper output value
                    job);
            //job.setCombinerClass(TwitterReducer.class);
            job.setReducerClass(TwitterReducer.class);
            //job.setMapperClass(TableHbaseMapper.class);
            //job.setMapOutputKeyClass(Text.class);
            //job.setMapOutputValueClass(IntWritable.class);
            //job.setOutputKeyClass(Text.class);
            //job.setOutputValueClass(IntWritable.class);
            job.setNumReduceTasks(3);
            job.setPartitionerClass(TwitterPartitioner.class);

            //FileInputFormat.addInputPath(job, new Path("/home/bernardo/NetBeansProjects/TwitterLambdaArchitecture/src/input.txt"));
            FileOutputFormat.setOutputPath(job, new Path("output" + String.valueOf(c) + ".txt"));
            c += 1;
            System.out.println("ciao3");
            //System.exit(job.waitForCompletion(true) ? 0 : 1);
            boolean b = job.waitForCompletion(true);
            if (!b) {
                throw new IOException("error with job!");
            }
            EndBatchCycle = System.currentTimeMillis();
            Utils.sleep(10000);
        }
        
    }
    
}
