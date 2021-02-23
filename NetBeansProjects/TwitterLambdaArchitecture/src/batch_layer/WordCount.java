/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package batch_layer;

import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 *
 * @author bernardo
 */
public class WordCount extends BaseBasicBolt {
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
  private static final Logger logger = LogManager.getLogger(WordCount.class);
  //For holding words and counts
  Map<String, Integer> counts = new HashMap<>();
  //How often to emit a count of words
  private Integer emitFrequency;

  // Default constructor
  public WordCount() {
      emitFrequency=5; // Default to 60 seconds
  }

  // Constructor that sets emit frequency
  public WordCount(Integer frequency) {
      emitFrequency=frequency;
  }

  //Configure frequency of tick tuples for this bolt
  //This delivers a 'tick' tuple on a specific interval,
  //which is used to trigger certain actions
  @Override
  public Map<String, Object> getComponentConfiguration() {
      Config conf = new Config();
      conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency);
      return conf;
  }

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
    String tweet=tuple.getStringByField("tweet");
    String label=tuple.getStringByField("label");
    String query=tuple.getStringByField("query");
    logger.info("Emitting a label of " + label + " for word " + tweet + " of query " + query);
    
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

    
}
