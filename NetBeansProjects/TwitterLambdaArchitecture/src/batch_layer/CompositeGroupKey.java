/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package batch_layer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 *
 * @author bernardo
 */
public class CompositeGroupKey implements
        WritableComparable<CompositeGroupKey> {
    private String tweet;
    private String label;
    
    public CompositeGroupKey(String c, String s){
        this.tweet=c;
        this.label=s;
    }

    public String getTweet() {
        return tweet;
    }

    public String getLabel() {
        return label;
    }
    
    public CompositeGroupKey(){
        this.tweet="";
        this.label="";
    }
    public void set(String c, String s){
        this.tweet=c;
        this.label=s;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, tweet);
        WritableUtils.writeString(out, label);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.tweet = WritableUtils.readString(in);
        this.label = WritableUtils.readString(in);
    }
    @Override
    public int compareTo(CompositeGroupKey pop) {
        if (pop == null)
            return 0;
        int intcnt = tweet.compareTo(pop.tweet);
        return intcnt == 0 ? label.compareTo(pop.label) : intcnt;
    }
    @Override
    public String toString() {
        return tweet.toString() + ":" + label.toString();
    }

    public static void main(String[] args) {
        String n="Bernardo";
        String c="tiezzi";
        CompositeGroupKey cgk = new CompositeGroupKey(n, c);
        String n1="bernardo";
        String c1="tiezzi";
        CompositeGroupKey cgk1 = new CompositeGroupKey(n1, c1);
        System.out.println(cgk.compareTo(cgk1));
        
        
    }
}