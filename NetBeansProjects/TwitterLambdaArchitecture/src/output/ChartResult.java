/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package output;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import javax.swing.BorderFactory;
import javax.swing.JFrame;
import java.awt.Color;
import java.awt.EventQueue;
import javax.swing.WindowConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.renderer.category.BarRenderer;
/**
 *
 * @author bernardo
 */
public class ChartResult extends JFrame{
    
    public ChartResult() {

        initUI();
    }

    private void initUI() {

        CategoryDataset dataset = createDataset();

        JFreeChart chart = createChart(dataset);
        ChartPanel chartPanel = new ChartPanel(chart);
        //chartPanel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));
        chartPanel.setBackground(Color.white);
        add(chartPanel);
        chartPanel.setPreferredSize(new java.awt.Dimension(800, 400));
        setContentPane(chartPanel);

        pack();
        setTitle("Bar chart");
        setLocationRelativeTo(null);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        
    }

    private CategoryDataset createDataset() {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        Configuration config = HBaseConfiguration.create(new Configuration());
        //HConnection connection = HConnectionManager.createConnection(config);
        //HTableInterface table = connection.getTable(TableName.valueOf("QueryTweetsAnalyzed"));
        //Table table= new HTable(config, TableName.valueOf("TWEETS"));
        config.set("hbase.zookeeper.property.clientPort", "2222");
        config.set("hbase.cluster.distributed", "true");
        config.set("hbase.rootdir", "hdfs://localhost:54310/hbase");
        int posCov=0;
        int negCov=0;
        int neuCov=0;
        int postru=0;
        int negtru=0;
        int neutru=0;
        int posBid=0;
        int negBid=0;
        int neuBid=0;
        
        try {
            HTable table= new HTable(config, TableName.valueOf("DatasetTweetsAnalyzed"));
            Scan s = new Scan();
            s.setCaching(1);
            //s.setStartRow(Bytes.toBytes("row00100001"));
            //s.setStopRow(Bytes.toBytes("row00100001"));
            
            s.addColumn(Bytes.toBytes("Id"), Bytes.toBytes("AccountID"));
            s.addColumn(Bytes.toBytes("Tweet"), Bytes.toBytes("Text"));
            s.addColumn(Bytes.toBytes("Tweet"), Bytes.toBytes("Label"));
            s.addColumn(Bytes.toBytes("Tweet"), Bytes.toBytes("Query"));
            ResultScanner scanner = table.getScanner(s);
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                //System.out.println("Found Row:" + result);
                byte[] value = result.getValue(Bytes.toBytes("Id"), Bytes.toBytes("AccountID"));
                byte[] value1 = result.getValue(Bytes.toBytes("Tweet"), Bytes.toBytes("Text"));
                byte[] value2 = result.getValue(Bytes.toBytes("Tweet"), Bytes.toBytes("Label"));
                byte[] value3 = result.getValue(Bytes.toBytes("Tweet"), Bytes.toBytes("Query"));

                String valueStr = Bytes.toString(value);
                String valueStr1 = Bytes.toString(value1);
                String valueStr2 = Bytes.toString(value2);
                String valueStr3 = Bytes.toString(value3);
                
                String row=Bytes.toString(result.getRow());
                System.out.println("Row "+row + " Id: " + valueStr + " Text: " + valueStr1+ " Label: " + valueStr2);
                if (valueStr3.equals("Covid")) {
                    if (valueStr2.equals("4") || valueStr2.equals("3")) {
                        posCov += 1;
                    } else if (valueStr2.equals("2")) {
                        neuCov += 1;
                    } else {
                        negCov += 1;
                    }

                }
                
                if (valueStr3.equals("Trump")) {
                    if (valueStr2.equals("4") || valueStr2.equals("3")) {
                        postru+= 1;
                    } else if (valueStr2.equals("2")) {
                        neutru += 1;
                    } else {
                        negtru += 1;
                    }

                }
                
                if (valueStr3.equals("Biden")) {
                    if (valueStr2.equals("4") || valueStr2.equals("3")) {
                        posBid += 1;
                    } else if (valueStr2.equals("2")) {
                        neuBid += 1;
                    } else {
                        negBid += 1;
                    }

                }
            }
            scanner.close();
            //}
        } catch (Exception ex) {

        }
        dataset.setValue(posCov, "Positives", "Covid");
        dataset.setValue(neuCov, "Neutrals", "Covid");
        dataset.setValue(negCov, "Negatives", "Covid");
        
        // Population in 2010
        dataset.addValue(postru, "Positives", "Trump");
        dataset.addValue(neutru, "Neutrals", "Trump");
        dataset.addValue(negtru, "Negatives", "Trump");

        // Population in 2015
        dataset.addValue(posBid, "Positives", "Biden");
        dataset.addValue(neuBid, "Neutrals", "Biden");
        dataset.addValue(negBid, "Negatives", "Biden");

        /*
        dataset.setValue(46, "Gold medals", "USA");
        dataset.setValue(38, "Gold medals", "China");
        dataset.setValue(29, "Gold medals", "UK");
        dataset.setValue(22, "Gold medals", "Russia");
        dataset.setValue(13, "Gold medals", "South Korea");
        dataset.setValue(11, "Gold medals", "Germany");
*/

        return dataset;
    }

    private JFreeChart createChart(CategoryDataset dataset) {

        JFreeChart barChart = ChartFactory.createBarChart(
                "Lambda Architecture Processing",
                "Valuations",
                "Number of Tweets",
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false);
        CategoryPlot plot = barChart.getCategoryPlot();
        
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setSeriesPaint(0, Color.red);
	renderer.setSeriesPaint(1, Color.yellow);
	renderer.setSeriesPaint(2, Color.blue);
	renderer.setDrawBarOutline(false);
	renderer.setItemMargin(0);

        return barChart;
    }

    public static void main(String[] args) {

        EventQueue.invokeLater(() -> {

            JFrame ex = new ChartResult();
            ex.setLocationRelativeTo(null);
            ex.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
            ex.setVisible(true);
        });
    }
}