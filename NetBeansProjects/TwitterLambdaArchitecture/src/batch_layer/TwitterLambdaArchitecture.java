/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package batch_layer;

import java.util.List;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.TwitterFactory;
import java.sql.*;
import java.text.Normalizer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import twitter4j.Paging;
import twitter4j.Query;
import twitter4j.QueryResult;

/**
 *
 * @author bernardo
 */
public class TwitterLambdaArchitecture {

    /**
     * @param args the command line arguments
     */
    static final String DB_URL = "jdbc:derby://localhost:1527/MyDatabase";

    //  Database credentials
    static final String USER = "Bernardo";
    static final String PASS = "bernardo";
    
    public static void main(String[] args) throws Exception {
        TwitterJob.main(args);
    }
    
}






