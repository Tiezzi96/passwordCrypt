/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package batch_layer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author bernardo
 */
public class TwitterPartitioner extends
        Partitioner< Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        String str = key.toString();
        if (numReduceTasks == 0) {
            return 0;
        }
        /*
        if (str.equals("#COVID19")) {
            return 0;
        } else if (str.equals("#BlackLivesMatter")) {
            return 1 % numReduceTasks;
        } else if (str.equals("#JoeBiden2020")) {
            return 2 % numReduceTasks;
        } else if (str.equals("#Israel")) {
            return 3 % numReduceTasks;
        } else if (str.equals("#5G")) {
            return 4 % numReduceTasks;
        }*/ if (str.equals("Trump")) {
            return 0;
           
        
        } else if (str.equals("Biden")) {
            return 1 % numReduceTasks;
           
        
        } else if (str.equals("Covid")) {
            return 2 % numReduceTasks;
           
        
        } else {
            return 0;
        }
    }
}