
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package speed_layer;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
//import org.apache.commons.codec.language.bm.Languages;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.FilterQuery;
import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.MediaEntity;
import twitter4j.Place;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.RateLimitStatus;
import twitter4j.Scopes;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.SymbolEntity;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStreamFactory;
import twitter4j.URLEntity;
import twitter4j.User;
import twitter4j.UserMentionEntity;
import twitter4j.conf.ConfigurationBuilder;

/**
 *
 * @author bernardo
 */
public class TwitterStreamSpout extends BaseRichSpout {

    SpoutOutputCollector _collector;
    Random _rand;
    //private twitter4j.Twitter twitter;
    private twitter4j.TwitterStream twitter;
    private LinkedBlockingQueue<Status> tweets;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setDebugEnabled(true)
                .setOAuthConsumerKey("YZcQGYa54T8Ueqg5tiXZjM3HZ")
                .setOAuthConsumerSecret("sDkH58gAGC9ePR0TTfVpKKH9i18aW84McUOUpvldpBPdqPmKvc")
                .setOAuthAccessToken("1284233485859860480-LvKw4EvYhwKVfqopmKCnc1bGAAeZIC")
                .setOAuthAccessTokenSecret("XG7uPiIt0VHnK5FCnAQK1rrFPWmzZSWrni8XdfnLjpOL0");
        TwitterStreamFactory tf = new TwitterStreamFactory(configurationBuilder.build());
        twitter = tf.getInstance();
        tweets= new LinkedBlockingQueue<>();
        final StatusListener statusListener = new StatusListener() {

                    @Override
                    public void onStatus(Status status) {
                        tweets.offer(status);
                    }

                    @Override
                    public void onDeletionNotice(StatusDeletionNotice sdn) {
                    }

                    @Override
                    public void onTrackLimitationNotice(int i) {
                     
                    }

                    @Override
                    public void onScrubGeo(long l, long l1) {
                     }

                    @Override
                    public void onStallWarning(StallWarning sw) {
                     }

                    @Override
                    public void onException(Exception excptn) {
                     }
                };
        twitter.addListener(statusListener);
        FilterQuery filter=new FilterQuery();
        filter.language("en");
        String[]query={"covid", "Trump", "Biden"};
        filter.track(query);
        twitter.filter(filter);
    }

    @Override
    public void nextTuple() {
        
        Utils.sleep(50);
        Status tweet=null;
        String sentence="";
        /*
        String[] sentences = new String[]{"#COVID19", "#BlackLivesMatter", "#JoeBiden2020", "#Israel", "#5G", "#BlackFriday"};//new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
        //"four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
        String[] sentences2=new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
        "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
        Query q = new Query(sentences[_rand.nextInt(sentences.length)]);
        q.setCount(1);
        q.setLang("en");
        System.out.println(q.getSince());
        String sentence = "bonanotte!";
        */
        try {
            /*
            System.out.println("query: " + q.getQuery());
            QueryResult queryResult = twitter.search(q);
            List<Status> status = queryResult.getTweets();
            
            sentence = status.get(0).getText();
            */
            tweet=tweets.poll();
            
        } catch (Exception e) {

        }
        if(tweet!=null){
            sentence=tweet.getText();
            if(!sentence.equals("")){
                sentence=tweet.getText();
                System.out.println("frase: " + sentence);
                _collector.emit(new Values(tweet));
            }
        }else{
            try {
                Thread.sleep(500);
                
            }catch(Exception e){
                
            }
            
        }
        /*while (sentence.isEmpty()) {
            //sentence=sentences2[_rand.nextInt(sentences2.length)];
            
            try {
                QueryResult queryResult = twitter.search(q);
                List<Status> status = queryResult.getTweets();
                sentence = status.get(0).getText();
            } catch (Exception e) {
                q = new Query(sentences[_rand.nextInt(sentences.length)]);
                q.setCount(1);
                q.setLang("en");
                System.out.println("query interna: " + q.getQuery());
            }
        }*/
        
    }
    
    @Override
    public void deactivate() {
        this._collector.emit(new Values(new Status() {
            @Override
            public Date getCreatedAt() {
                return null;
            }

            @Override
            public long getId() {
                return 0;
            }

            @Override
            public String getText() {
                
                return "TweetStreamSpout is been deactivated";
            }

            @Override
            public int getDisplayTextRangeStart() {
                return 0;
            }

            @Override
            public int getDisplayTextRangeEnd() {
                return 0;
            }

            @Override
            public String getSource() {
                return null;
            }

            @Override
            public boolean isTruncated() {
                return false;
            }

            @Override
            public long getInReplyToStatusId() {
                return 0;
            }

            @Override
            public long getInReplyToUserId() {
                return 0;
            }

            @Override
            public String getInReplyToScreenName() {
                return null;
            }

            @Override
            public GeoLocation getGeoLocation() {
                return null;
            }

            @Override
            public Place getPlace() {
                return null;
            }

            @Override
            public boolean isFavorited() {
                return false;
            }

            @Override
            public boolean isRetweeted() {
                return false;
            }

            @Override
            public int getFavoriteCount() {
                return 0;
            }

            @Override
            public User getUser() {
                return null;
            }

            @Override
            public boolean isRetweet() {
                return false;
            }

            @Override
            public Status getRetweetedStatus() {
                return null;
            }

            @Override
            public long[] getContributors() {
                return null;
            }

            @Override
            public int getRetweetCount() {
                return 0;
            }

            @Override
            public boolean isRetweetedByMe() {
                return false;
            }

            @Override
            public long getCurrentUserRetweetId() {
                return 0;
            }

            @Override
            public boolean isPossiblySensitive() {
                return false;
            }

            @Override
            public String getLang() {
                return null;
            }

            @Override
            public Scopes getScopes() {
                return null;
            }

            @Override
            public String[] getWithheldInCountries() {
                return null;
            }

            @Override
            public long getQuotedStatusId() {
                return 0;
            }

            @Override
            public Status getQuotedStatus() {
                return null;
            }

            @Override
            public URLEntity getQuotedStatusPermalink() {
                return null;
            }

            @Override
            public int compareTo(Status o) {
                return 0;
            }

            @Override
            public RateLimitStatus getRateLimitStatus() {
                return null;
            }

            @Override
            public int getAccessLevel() {
                return 0;
            }

            @Override
            public UserMentionEntity[] getUserMentionEntities() {
                return null;
            }

            @Override
            public URLEntity[] getURLEntities() {
                return null;
            }

            @Override
            public HashtagEntity[] getHashtagEntities() {
                return null;
            }

            @Override
            public MediaEntity[] getMediaEntities() {
                return null;
            }

            @Override
            public SymbolEntity[] getSymbolEntities() {
                return null;
            }
            })); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }
    
    @Override
    public final void close() {
        this.twitter.cleanUp();
        this.twitter.shutdown();
    }

}