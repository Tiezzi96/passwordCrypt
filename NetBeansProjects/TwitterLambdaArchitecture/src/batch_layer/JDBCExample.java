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

//STEP 1. Import required packages
import java.sql.*;

public class JDBCExample {
   // JDBC driver name and database URL  
   static final String DB_URL = "jdbc:derby://localhost:1527/MyDatabase";

   //  Database credentials
   static final String USER = "Bernardo";
   static final String PASS = "bernardo";
   
   public static void main(String[] args){
   Connection conn = null;
   Statement stmt = null;
   try{
      //STEP 2: Register JDBC driver
      //Class.forName("com.mysql.jdbc.Driver");

      //STEP 3: Open a connection
      System.out.println("Connecting to a selected database...");
      conn = DriverManager.getConnection(DB_URL, USER, PASS);
      System.out.println("Connected database successfully...");
      
      //STEP 4: Execute a query
      System.out.println("Creating table in given database...");
      stmt = conn.createStatement();
      
      String sql = "CREATE TABLE QueryTweets"+
              "(ID INT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),"+
              "AccountID VARCHAR(255),"+
              "Date TIMESTAMP,"+
              "Query VARCHAR(255),"+
              "Text VARCHAR(1023),"+
              "Label INT,"+
              "PRIMARY KEY(ID))"; 
       System.out.println(sql);

      stmt.executeUpdate(sql);
      System.out.println("Created table in given database...");
   }catch(SQLException se){
      //Handle errors for JDBC
      se.printStackTrace();
   }catch(Exception e){
      //Handle errors for Class.forName
      e.printStackTrace();
   }finally{
      //finally block used to close resources
      try{
         if(stmt!=null)
            conn.close();
      }catch(SQLException se){
      }// do nothing
      try{
         if(conn!=null)
            conn.close();
      }catch(SQLException se){
         se.printStackTrace();
      }//end finally try
   }//end try
   System.out.println("Goodbye!");
}//end main
}//end JDBCExample