/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package speed_layer;

/**
 *
 * @author bernardo
 */
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.Status;

/**
 *
 * @author bernardo
 */
public class TwitterHbaseBolt extends BaseBasicBolt {

    public static HTable table;
    public static volatile String lastrow = "";
    private static AtomicInteger count = new AtomicInteger(0);
    public static AtomicBoolean accesstimestamp = new AtomicBoolean();
    public static AtomicBoolean accessCount = new AtomicBoolean(true);
    public static Configuration config;

    /*
     Map<String, Integer> counts = new HashMap<>();
    
     @Override
     public void execute(Tuple tuple, BasicOutputCollector collector) {
     String word = tuple.getString(0);
     //System.out.println("word execute: " + word);
     Integer count = counts.get(word);
     if (count == null) {
     count = 0;
     }
     count++;
     counts.put(word, count);
     collector.emit(new Values(word, count));
     //System.out.println("wordcount execute: "+word+" "+count);
     }
     @Override
     public void declareOutputFields(OutputFieldsDeclarer declarer) {
     declarer.declare(new Fields("word", "count"));
     }*/
    //Create logger for this class
    private static final Logger logger = LogManager.getLogger(TwitterHbaseBolt.class);
    //For holding words and counts
    Map<String, Integer> counts = new HashMap<>();
    //How often to emit a count of words
    private Integer emitFrequency;

    // Default constructor
    public TwitterHbaseBolt() {
        //emitFrequency=5; // Default to 60 seconds
    }

    // Constructor that sets emit frequency
    public TwitterHbaseBolt(Integer frequency) {
        emitFrequency = frequency;
    }

    public void setTimeStamp(long time, String r) throws IOException {
        HTable t = new HTable(config, "LastRowTimeStamp");
        Put p = new Put(Bytes.toBytes("row 0001"));
        p.add(Bytes.toBytes("Time"), Bytes.toBytes("TimeValue"), Bytes.toBytes(String.valueOf(time)));
        p.add(Bytes.toBytes("Row"), Bytes.toBytes("RowValue"), Bytes.toBytes(r));
        t.put(p);
        System.out.println("LastRowTimeStamp is " + String.valueOf(time));
    }

    //Configure frequency of tick tuples for this bolt
    //This delivers a 'tick' tuple on a specific interval,
    //which is used to trigger certain actions
    /*
     @Override
     public Map<String, Object> getComponentConfiguration() {
     Config conf = new Config();
     conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency);
     return conf;
     }
     */
    //execute is called to process tuples
    /*
     @Override
     public void execute(Tuple tuple, BasicOutputCollector collector) {
     //If it's a tick tuple, emit all words and counts
     if(tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
     && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
     for(String word : counts.keySet()) {
     Integer count = counts.get(word);
     collector.emit(new Values(word, count));
     logger.info("Emitting a count of " + count + " for word " + word);
     }
     } else {
     //Get the word contents from the tuple
     String word = tuple.getString(0);
     //Have we counted any already?
     Integer count = counts.get(word);
     if (count == null)
     count = 0;
     //Increment the count and store it
     count++;
     counts.put(word, count);
     }
     }*/
    //Declare that this emits a tuple containing two fields; word and count
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if (!lastrow.equals("")) {
            System.out.println("lastrow");
            String query = tuple.getStringByField("query");
            Status tweet = (Status) tuple.getValueByField("tweet");
            String label = tuple.getStringByField("label");
            if (query == null || tweet == null || label == null) {
                System.out.println("Tweet is null!");
            } else {
                String text = tweet.getText();
                int id = count.getAndIncrement();
                DecimalFormat decimalFormat = new DecimalFormat("00000000");
                String ID = decimalFormat.format(id);
                Put p = new Put(Bytes.toBytes("row" + ID));
                System.out.println("Query is: " + query);
                p.add(Bytes.toBytes("Id"), Bytes.toBytes("AccountID"), Bytes.toBytes(String.valueOf(tweet.getId())));
                p.add(Bytes.toBytes("Date"), Bytes.toBytes("CreationDate"), Bytes.toBytes(tweet.getCreatedAt().toString()));
                p.add(Bytes.toBytes("Tweet"), Bytes.toBytes("Query"), Bytes.toBytes(query));
                p.add(Bytes.toBytes("Tweet"), Bytes.toBytes("Text"), Bytes.toBytes(text));
                p.add(Bytes.toBytes("Tweet"), Bytes.toBytes("Label"), Bytes.toBytes(label));
                try {
                    table.put(p);
                    System.out.println("ho inserito la row" + ID);
                    String row = "row" + ID;
                    Get g = new Get(Bytes.toBytes("row" + ID));
                    Result r = table.get(g);
                    System.out.println("row ID: " + "row" + ID);
                    setTimeStamp(r.rawCells()[0].getTimestamp(), row);

                } catch (Exception e) {
                    System.out.println("non ho inserito la row" + ID);
                }
                //logger.info("Emitting a label of " + label + " for word " + tweet + " of query " + query);
                System.out.println("Emitting a label of " + label + " for word " + text + " of query " + query);
                collector.emit(new Values(query, tweet, label));

            }

        } else {
            System.out.println("I'm waiting");
        }
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2222");

        conf.set("hbase.cluster.distributed", "true");
        conf.set("hbase.rootdir", "hdfs://localhost:54310/hbase");
        accesstimestamp.set(true);
        if (accessCount.get()) {
            accessCount.set(false);
            config = conf;
            try {
                table = new HTable(conf, TableName.valueOf("DatasetTweetsAnalyzed"));
                Scan s = new Scan();
                System.out.println("Bernardo_Table is 1");
                s.setCaching(1);
                ResultScanner scanner = table.getScanner(s);
                for (Result result = scanner.next(); result != null; result = scanner.next()) {
                    //System.out.println("Found Row:" + result);
                    String row = Bytes.toString(result.getRow());
                    System.out.println("Row " + row);
                    lastrow = row;
                }
                scanner.close();
                //}
            } catch (Exception e) {
                System.out.println("Bernardo_Table is 2");
                e.printStackTrace();
            }
            if (!lastrow.equals("")) {
                StringBuilder sb = new StringBuilder(lastrow);
                sb.delete(0, 3);
                while (sb.charAt(0) == '0') {
                    sb.deleteCharAt(0);
                }
                System.out.println("lastrow is " + sb.toString());
                lastrow = sb.toString();
                count.set(Integer.parseInt(lastrow));
            }
            accessCount.set(true);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("query", "tweet", "label"));
    }

    /*
     public static void main(String[] args) throws Exception {
     //System.setProperty("storm.jar", "/opt/apache-storm-2.2.0/lib/storm-client-2.2.0.jar");
     TopologyBuilder builder = new TopologyBuilder();
     builder.setSpout("spout", new RandomSentenceSpout(), 1);//5
     builder.setBolt("split", new SplitSentence(), 1).shuffleGrouping("spout");//8
     builder.setBolt("count", new WordCount(), 1).fieldsGrouping("split", new Fields("word"));//12
     Config conf = new Config();
     //conf.setMessageTimeoutSecs(120);
     conf.setDebug(false);
     if (args != null && args.length > 0) {
     conf.setNumWorkers(3);
     StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
     } else {
     conf.setMaxTaskParallelism(3);
     final LocalCluster cluster = new LocalCluster();
     try {
     cluster.submitTopology("word-count", conf, builder.createTopology());
     //System.setProperty("storm.jar", "/opt/apache-storm-2.2.0/lib/storm-client-2.2.0.jar");
     //StormSubmitter.submitTopology("word-count2", conf, builder.createTopology());
     Utils.sleep(10000);
     //Thread.sleep(60000);
     // System.out.println("finish!");
     cluster.killTopology("word-count");
     } finally {
     System.out.println("finish!");
     //cluster.killTopology("word-count");
     cluster.shutdown();
     //cluster.close();
                
     //System.exit(0);
     }
        
     }
     System.out.println("finish!");
     }*/
    @Override
    public void cleanup() {
        super.cleanup();
        lastrow = "";
        count.set(0);//To change body of generated methods, choose Tools | Templates.
    }
}
