/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package passwordcrypt;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.block.BlockBorder;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import javax.swing.BorderFactory;
import javax.swing.JFrame;
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.EventQueue;
import java.awt.Font;
import java.awt.Shape;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import org.jfree.chart.axis.LogAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.NumberTickUnit;
import org.jfree.util.ShapeUtilities;
import static passwordcrypt.DesEncrypterThread.passwordcryptparallel;

/**
 *
 * @author bernardo
 */
public class LineChartOutput extends JFrame {

    public LineChartOutput() {
        try {
            initUI();
        } catch (Exception e) {

        }
    }

    private void initUI() throws Exception {

        XYDataset dataset = createDataset2();
        JFreeChart chart = createChart(dataset);
        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));
        chartPanel.setBackground(Color.white);
        add(chartPanel);

        pack();
        setTitle("Line chart");
        setLocationRelativeTo(null);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }

    private XYDataset createDataset() {
        /*

        XYSeries series1 = new XYSeries("2014");
        series1.add(18, 530);
        series1.add(20, 580);
        series1.add(25, 740);
        series1.add(30, 901);
        series1.add(40, 1300);
        series1.add(50, 2219);
         */
        XYSeries series2 = new XYSeries("2016");
        series2.add(2, 2);
        series2.add(3, 3);
        series2.add(5, 20);
        series2.add(10, 11);
        series2.add(15, 6);
        series2.add(20, 15);
        series2.add(800, 8);
        series2.add(980, 9);
        series2.add(1210, 10);
        series2.add(2350, 12);

        XYSeriesCollection dataset = new XYSeriesCollection();
        //dataset.addSeries(series1);
        dataset.addSeries(series2);

        return dataset;
    }

    private XYDataset createDataset2() throws Exception {
        File file = new File("src/passwordcrypt/dataset_password.txt");
        FileReader fr = new FileReader(file);
        String strLine = "";
        List<String> list = new ArrayList<String>();
        BufferedReader br = new BufferedReader(fr);
        while (strLine != null) {
            strLine = br.readLine();
            if (strLine == null) {
                break;
            }
            list.add(strLine);
        }
        String[] str = new String[list.size()];
        Iterator<String> iter = list.iterator();
        int i = 0;
        while (iter.hasNext()) {
            str[i] = iter.next();
            i += 1;
        }
        br.close();
        SecretKey key = KeyGenerator.getInstance("DES").generateKey();
        System.out.println(key);
        DesEncrypter encrypter = new DesEncrypter(key);
        int ThreadPoolSize = Runtime.getRuntime().availableProcessors() * 2;
        System.err.println("ThreadPoolSize: " + ThreadPoolSize);
        int[] num = {2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 128, 256, 512, 1024};
        HashMap<String, String> ExecutionTimes = DesEncrypterThread.passwordcryptparallelRandom(num, str, key, encrypter);
        XYSeries series2 = new XYSeries("SpeedUp Threads");
        Iterator itr = ExecutionTimes.entrySet().iterator();
        while (itr.hasNext()) {
            HashMap.Entry values = (HashMap.Entry) itr.next();
            System.out.println(values.getKey().toString() + " , " + Double.valueOf(values.getValue().toString()));
            series2.add(Integer.parseInt(values.getKey().toString()),
                    Double.valueOf(values.getValue().toString()));
        }
        XYSeriesCollection dataset = new XYSeriesCollection();
        //dataset.addSeries(series1);
        dataset.addSeries(series2);

        return dataset;

    }
    
    private XYDataset createDataset3() throws Exception {
        File file = new File("src/passwordcrypt/value_speedup_random_password.txt");
        FileReader fr = new FileReader(file);
        String strLine = "";
        List<String> list = new ArrayList<String>();
        BufferedReader br = new BufferedReader(fr);
        while (strLine != null) {
            strLine = br.readLine();
            if (strLine == null) {
                break;
            }
            list.add(strLine);
        }
        double[] data = new double[list.size()];
        Iterator<String> iter = list.iterator();
        int i = 0;
        while (iter.hasNext()) {
            data[i] = Double.valueOf(iter.next());
            i += 1;
        }
        br.close();
        int[] num = {2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 128, 256, 512, 1024};
        XYSeries series2 = new XYSeries("SpeedUp Threads");
        for(int j=0; j<data.length;j++){
            System.out.println(data[j]);
            series2.add(num[j],
                    data[j]);
        }
        XYSeriesCollection dataset = new XYSeriesCollection();
        //dataset.addSeries(series1);
        dataset.addSeries(series2);

        return dataset;

    }

    private JFreeChart createChart(final XYDataset dataset) {

        JFreeChart chart = ChartFactory.createXYLineChart(
                "SpeedUp Password Decrypt",
                "Threads",
                "SpeedUp",
                dataset,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );
        LogAxis xAxis = new LogAxis("Threads");
        xAxis.setBase(10);
        xAxis.setTickUnit(new NumberTickUnit(1));
        xAxis.setTickLabelsVisible(true);
        XYPlot plot = chart.getXYPlot();
        DecimalFormat formatter=new DecimalFormat("####.##",new DecimalFormatSymbols(new Locale("us")));
        xAxis.setNumberFormatOverride(formatter);
        plot.setDomainAxis(xAxis);

        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();

        renderer.setSeriesPaint(0, Color.BLUE);
        renderer.setSeriesStroke(0, new BasicStroke(1.5f));
        Shape s = ShapeUtilities.createDiamond(3);
        renderer.setSeriesShape(0, s);
        //renderer.setSeriesPaint(1, Color.BLUE);
        //renderer.setSeriesStroke(1, new BasicStroke(2.0f));

        plot.setRenderer(renderer);
        plot.setBackgroundPaint(Color.white);
        plot.setRangeGridlinesVisible(true);

        plot.setDomainGridlinesVisible(true);
        plot.setDomainGridlinePaint(Color.BLACK);
        plot.setRangeGridlinesVisible(true);
        plot.setRangeGridlinePaint(Color.BLACK);
        
       

        chart.getLegend().setFrame(BlockBorder.NONE);

        chart.setTitle(new TextTitle("SpeedUp Random password Decrypt",
                new Font("Serif", Font.BOLD, 18)
        )
        );

        return chart;
    }

    public static void main(String[] args) {

        EventQueue.invokeLater(() -> {

            JFrame ex = new LineChartOutput();
            ex.setVisible(true);
        });
    }
}
