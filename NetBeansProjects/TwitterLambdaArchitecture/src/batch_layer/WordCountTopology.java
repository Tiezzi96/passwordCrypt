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
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class WordCountTopology {

    //Entry point for the topology
    public static void main(String[] args) throws Exception {
        //Used to build the topology
        TopologyBuilder builder = new TopologyBuilder();
        //Add the spout, with a name of 'spout'
        //and parallelism hint of 5 executors
        builder.setSpout("spout", new RandomSentenceSpout(), 1);
        //Add the SplitSentence bolt, with a name of 'split'
        //and parallelism hint of 8 executors
        //shufflegrouping subscribes to the spout, and equally distributes
        //tuples (sentences) across instances of the SplitSentence bolt
        //builder.setBolt("split", new SplitSentence(), 1).shuffleGrouping("spout");
        builder.setBolt("split", new SplitSentence(), 1).shuffleGrouping("spout");
        //Add the counter, with a name of 'count'
        //and parallelism hint of 12 executors
        //fieldsgrouping subscribes to the split bolt, and
        //ensures that the same word is sent to the same instance (group by field 'word')
        //builder.setBolt("count", new WordCount(), 1).fieldsGrouping("split", new Fields("query"));
        NLP.init();

        //new configuration
        Config conf = new Config();
        //Set to false to disable debug information when
        // running in production on a cluster
        conf.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        //If there are arguments, we are running on a cluster
        if (args != null && args.length > 0) {
            //parallelism hint to set the number of workers
            conf.setNumWorkers(3);
            //submit the topology
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } //Otherwise, we are running locally
        else {
            //Cap the maximum number of executors that can be spawned
            //for a component to 3
            //conf.setMaxTaskParallelism(3);
            //LocalCluster is used to run locally
            //submit the topology
            cluster.submitTopology("word-counted", conf, builder.createTopology());
            //sleep
            Thread.sleep(20000);
            //shut down the cluster
            cluster.killTopology("word-counted");
            //Utils.sleep(5000);
            System.out.println("becat"+cluster.getClusterInfo());
            cluster.shutdown();
            System.exit(0);
            
        }
    }
}
