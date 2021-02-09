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

/**
 *
 * @author bernardo
 */
public class DesEncrypterRunnable implements Runnable {

    private DesEncrypter encrypter;
    private String dataset[];
    private String password;
    private String passwordcrypted;
    private AtomicInteger numPassword;
    private AtomicLong timeout;
    private String name;
    private AtomicBoolean passCheck;

    public DesEncrypterRunnable(String name, String str[], String password, AtomicInteger num, AtomicLong timeout, AtomicBoolean b, DesEncrypter encrypter) {
        this.name = name;
        this.dataset = str;
        this.passwordcrypted = password;
        this.numPassword = num;
        this.encrypter = encrypter;
        this.timeout = timeout;
        this.passCheck = b;

    }
/*
    public void run() {
        try {
            //System.out.println("entra");
            //System.out.println(dataset.length);
            int count=0;
            long Time=0;
            long Time2=0;
            for (int j = 0; j < dataset.length; j++) {
                Time=System.currentTimeMillis();
                
                //System.out.println(j);
                //System.out.println("entra 2");
                if (passwordcrypted.equals(encrypter.encrypt(dataset[j]))) {
                    password = dataset[j];
                    numPassword.decrementAndGet();
                    System.out.println("Thread " + this.name);
                    System.out.println("oi" + password);
                    timeout.set(System.currentTimeMillis());
                    passCheck.set(true);
                    //System.out.println("ii");  
                }
                //System.out.println(numPassword.get());

                if (numPassword.get() == 0) {
                    //System.out.println(numPassword.get());
                    //System.out.println(timeout.get());
                    break;
                }
                Time2+=(System.currentTimeMillis()-Time);
                count+=1;
            }
            System.out.println("Thread "+this.name+": "+(((double)Time2/(double)count)));

        } catch (Exception e) {
            System.out.println("Interrupted");
        } finally {
            System.out.println("Finisced");
        }
    }*/
    
    public void run() {
        try {
            //System.out.println("entra");
            //System.out.println(dataset.length);
            int count=0;
            long Time=0;
            long Time2=0;
            int j=0;
            while( j < dataset.length) {
                if(passCheck.get()){
                    passCheck.set(false);
                    
                String crypt=encrypter.encrypt(dataset[j]);
                Time=System.currentTimeMillis();
                
                //System.out.println(j);
                //System.out.println("entra 2");
                passCheck.set(true);
                if (passwordcrypted.equals(crypt)) {
                    password = dataset[j];
                    //System.out.println("ii"); 
                    numPassword.decrementAndGet();
                }
                //System.out.println(numPassword.get());

                if (numPassword.get() == 0) {
                    //System.out.println(numPassword.get());
                    //System.out.println(timeout.get());
                    break;
                }
                Time2+=(System.currentTimeMillis()-Time);
                count+=1;
                j+=1;
            }
            }
            System.out.println("Thread "+this.name+": "+(((double)Time2/(double)count)));

        } catch (Exception e) {
            System.out.println("Interrupted");
        } finally {
            System.out.println("Finisced");
        }
    }

    public static void main(String[] args) throws Exception {
        File file = new File("src/passwordcrypt/dataset_password.txt");
        FileReader fr = new FileReader(file);
        //StringBuilder sb = new StringBuilder();
        String strLine = "";
        List<String> list = new ArrayList<String>();
        BufferedReader br = new BufferedReader(fr);
        while (strLine != null) {
            //strLine = br.readLine();
            //sb.append(strLine);
            //sb.append(System.lineSeparator());
            strLine = br.readLine();
            if (strLine == null) {
                break;
            }
            list.add(strLine);
        }
        String str[] = new String[list.size()];
        Iterator<String> iter = list.iterator();
        int i = 0;
        while (iter.hasNext()) {
            str[i] = iter.next();
            i += 1;
        }
        br.close();
        SecretKey key = KeyGenerator.getInstance("DES").generateKey();
        System.out.println(key);
        System.out.println(str[(int) (((1 + 1.0) / 4) * str.length - 1)]);
        //String s[]= Arrays.copyOfRange(str, 1, (1/4)*str.length);
        DesEncrypter encrypter = new DesEncrypter(key);
        ExecutorService esecutore = Executors.newFixedThreadPool(4);
        AtomicBoolean bool = new AtomicBoolean(true);
        AtomicInteger num = new AtomicInteger(1);
        AtomicLong timeout = new AtomicLong();
        ArrayList<String[]> l = new ArrayList<String[]>();
        for (int h = 0; h < 4; h++) {
            l.add(Arrays.copyOfRange(str, (int) (Math.round(((h + 0.0) / 4) * str.length)), (int) Math.round(((h + 1.0) / 4) * str.length)));
        }
        String passCrypted = encrypter.encrypt(str[(str.length -1)]);
        str=null;
        long initTime = System.currentTimeMillis();
        for (i = 0; i < 4; i++) {
            esecutore.submit(new DesEncrypterRunnable("" + i + "", l.get(i), passCrypted, num, timeout, bool, encrypter));
            //System.out.println(DesEncrypterThread.timeout);
        }
        try {
            esecutore.shutdown();
            System.out.println(Long.MAX_VALUE+" "+TimeUnit.NANOSECONDS);
            esecutore.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            long endTime = System.currentTimeMillis();
            System.out.println("ciao " + String.valueOf(endTime - initTime));
            System.out.println("ciao " + String.valueOf(timeout.get() - initTime));
        } catch (InterruptedException e) {

        }
    }

}
