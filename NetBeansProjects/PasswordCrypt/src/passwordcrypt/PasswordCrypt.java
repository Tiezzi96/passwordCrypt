/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package passwordcrypt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import passwordcrypt.DesEncrypterThread;


/**
 *
 * @author bernardo
 */
public class PasswordCrypt {

    /**
     * @param args the command line arguments
     */
            
    /*
    public static void main(String[] args) throws Exception {
        File file = new File("src/passwordcrypt/dataset_password.txt");
        FileReader fr = new FileReader(file);
        //StringBuilder sb = new StringBuilder();
        String strLine = "";
        List<String> list = new ArrayList<String>();
        BufferedReader br = new BufferedReader(fr);
        while (strLine != null)
        {
            //strLine = br.readLine();
            //sb.append(strLine);
            //sb.append(System.lineSeparator());
            strLine = br.readLine();
            if (strLine==null)
                break;
            list.add(strLine);
        }
        String str[]= new String[list.size()];
        Iterator <String> iter = list.iterator();
        int i=0;
        while(iter.hasNext()){
            str[i]=iter.next();
            i+=1;
        }
        br.close();
        SecretKey key = KeyGenerator.getInstance("DES").generateKey();
        System.out.println(key);
        System.out.println(str[(int)(((1+1.0)/4)*str.length-1)]);
        //String s[]= Arrays.copyOfRange(str, 1, (1/4)*str.length);
        DesEncrypter encrypter = new DesEncrypter(key);
        ExecutorService esecutore = Executors.newFixedThreadPool(4);
        AtomicBoolean bool= new AtomicBoolean(false);
        AtomicInteger num= new AtomicInteger(1);
        AtomicLong timeout= new AtomicLong();
        long timein=System.currentTimeMillis();
        for(i=0; i<4; i++){
            esecutore.execute(new DesEncrypterThread("thread", Arrays.copyOfRange(str, (int)(((i+0.0)/4)*str.length), (int)(((i+1.0)/4)*str.length)),encrypter.encrypt(str[str.length-1]),num,timeout,bool, encrypter));
        }
        esecutore.shutdown();
    
    }*/
    
    
    
    public static void main(String[] args) throws Exception {
        File file = new File("src/passwordcrypt/dataset_password.txt");
        FileReader fr = new FileReader(file);
        //StringBuilder sb = new StringBuilder();
        String strLine = "";
        List<String> list = new ArrayList<String>();
        BufferedReader br = new BufferedReader(fr);
        while (strLine != null)
        {
            //strLine = br.readLine();
            //sb.append(strLine);
            //sb.append(System.lineSeparator());
            strLine = br.readLine();
            if (strLine==null)
                break;
            list.add(strLine);
        }
        
        File file2 = new File("src/passwordcrypt/dataset_password.txt");
        FileReader fr2 = new FileReader(file2);
        //StringBuilder sb = new StringBuilder();
        String strLine2 = "";
        List<String> list2 = new ArrayList<String>();
        BufferedReader br2 = new BufferedReader(fr2);
        while (strLine2 != null)
        {
            //strLine = br.readLine();
            //sb.append(strLine);
            //sb.append(System.lineSeparator());
            strLine2 = br2.readLine();
            if (strLine2==null)
                break;
            list2.add(strLine2);
        }
        System.out.println(list.size());
        Iterator<String> itr=list2.iterator();
        int count=0;
        while(itr.hasNext()){
            count+=1;
            String value=itr.next();
            if(!list.contains(value)){
                list.add(value);
            }
            if(count%10000==0){
                System.out.println("count "+count);
                System.out.println("list.size() "+list.size());

            }
        }
        System.out.println(list.size());
        
    }
    
}

