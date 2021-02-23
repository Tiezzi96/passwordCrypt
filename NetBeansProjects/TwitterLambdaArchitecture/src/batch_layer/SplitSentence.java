/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package batch_layer;

import java.text.BreakIterator;
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 *
 * @author bernardo
 */
public class SplitSentence extends BaseBasicBolt {
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("query","tweet","label"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
/*
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {*/
        /*
      //System.out.println("sentence execute");
      String sentence = tuple.getStringByField("sentence");
      String words[] = sentence.split(" ");
      for (String w : words) {
        basicOutputCollector.emit(new Values(w));
        //System.out.println("word execute: "+w);
      }*//*
        //Get the sentence content from the tuple
        String sentence = tuple.getString(0);
        //An iterator to get each word
        BreakIterator boundary = BreakIterator.getWordInstance();
        //Give the iterator the sentence
        boundary.setText(sentence);
        //Find the beginning first word
        int start = boundary.first();
        //Iterate over each word and emit it to the output stream
        for (int end = boundary.next(); end != BreakIterator.DONE; start = end, end = boundary.next()) {
            //get the word
            String word = sentence.substring(start, end);
            //If a word is whitespace characters, replace it with empty
            word = word.replaceAll("\\s+", "");
            //if it's an actual word, emit it
            if (!word.equals("")) {
                basicOutputCollector.emit(new Values(word));
            }
        }
    }*/
    
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String tweet=tuple.getStringByField("sentence");
        int label=NLP.findSentiment(tweet);
        String query=tuple.getStringByField("query");
        System.out.println(tweet+": "+label);
        basicOutputCollector.emit(new Values(query,tweet,String.valueOf(label)));
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        super.prepare(topoConf, context); //To change body of generated methods, choose Tools | Templates.    
    }
    
    
}
