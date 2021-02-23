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
import java.io.*;
import java.sql.*;
 
/**
 * A simple Java program that exports data from database to CSV file.
 * @author Nam Ha Minh
 * (C) Copyright codejava.net
 */
public class SimpleDb2CsvExporter {
 
    public static void main(String[] args) {
        String jdbcURL = "jdbc:derby://localhost:1527/MyDatabase";
        String username = "Bernardo";
        String password = "bernardo";
         
        String csvFilePath = "QUERYTWEETS-export.csv";
         
        try (Connection connection = DriverManager.getConnection(jdbcURL, username, password)) {
            String sql = "SELECT * FROM QUERYTWEETS";
             
            Statement statement = connection.createStatement();
             
            ResultSet result = statement.executeQuery(sql);
             
            BufferedWriter fileWriter = new BufferedWriter(new FileWriter(csvFilePath));
             
            // write header line containing column names       
            fileWriter.write("ID,ACCOUNTID,DATE,QUERY,TEXT,LABEL");
             
            while (result.next()) {
                int id = result.getInt("ID");
                String accountid = result.getString("ACCOUNTID");
                Timestamp date = result.getTimestamp("DATE");
                String query = result.getString("QUERY");
                String text = result.getString("TEXT");
                int label = result.getInt("LABEL");
                 
                if (text == null) {
                    text = "";   // write empty value for null
                } else {
                    text = "\"" + text + "\""; // escape double quotes
                }
                
                if (accountid == null) {
                    accountid = "";   // write empty value for null
                } else {
                    accountid = "\"" + accountid + "\""; // escape double quotes
                }
                
                if (query == null) {
                    query = "";   // write empty value for null
                } else {
                    query = "\"" + query + "\""; // escape double quotes
                }
                 
                String line = String.format("\"%s\",%s,%s,%s,%s, \"%s\"",
                        id, accountid, query, date, text, label);
                 
                fileWriter.newLine();
                fileWriter.write(line);            
            }
             
            statement.close();
            fileWriter.close();
             
        } catch (SQLException e) {
            System.out.println("Datababse error:");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("File IO error:");
            e.printStackTrace();
        }
         
    }
 
}
