/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package passwordcrypt;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.codec.binary.Base64;
/**
 *
 * @author bernardo
 */
public class DesEncrypterThread extends Thread{
    private DesEncrypter encrypter;
    private static String dataset[];
    private String password;
    private String passwordcrypted;
    private byte[] utf8passwordcrypted;
    private AtomicInteger numPassword;
    private AtomicLong timeout;
    private String name;
    private AtomicBoolean passCheck;
    private int numThreads;
    private SecretKey Key;
    
    
    public DesEncrypterThread(String name, int threads, String password, AtomicInteger num, AtomicLong timeout, AtomicBoolean b, SecretKey key, byte []utf8password) throws  Exception{
        this.name=name;
        this.passwordcrypted=password;
        this.utf8passwordcrypted=utf8password;
        this.numPassword=num;
        this.encrypter=new DesEncrypter(key);
        this.timeout=timeout;
        this.passCheck=b;
        this.numThreads=threads;
        this.Key=key;
    }
    
    public static void setdataset(String[] str){
        dataset = str;
    }
    public void run(){
        try{
            DesEncrypter encrypt=new DesEncrypter(Key);
            int inizio=(int)((Integer.valueOf(name)+0.0)/numThreads*dataset.length);
            int fine=(int)((Integer.valueOf(name)+1.0)/numThreads*dataset.length);
            //utf8passwordcrypted=Base64.decodeBase64(passwordcrypted);
            for(int j=inizio; j<fine; j++){
                //encrypt.encrypt(dataset[j]);
                
                if(passwordcrypted.equals(encrypt.encrypt(dataset[j]))){
                    password=dataset[j];
                    numPassword.decrementAndGet();
                    System.out.println("Thread "+this.name);
                    System.out.println("Password found: "+password);
                    timeout.set(System.nanoTime());
                    passCheck.set(true);
                }
                //System.out.println(numPassword.get());
                
                if(numPassword.get() == 0){
                    //System.out.println(numPassword.get());
                    //System.out.println(timeout.get());
                    break;
                }
                
            }
            
        }catch(Exception e){
            System.out.println("Interrupted");
        }finally{
            System.out.println("Finisced");
        }
    }
    
    public static HashMap passwordcryptparallel(int [] num, String [] s, SecretKey k, DesEncrypter encrypter) throws Exception{
        String [] str=s;
        int [] Threads=num;
        int test = 25;
        //FileWriter myWriter = new FileWriter("src/passwordcrypt/value_speedup_first_password.txt");
        HashMap<String, String> ExecutionTime=new HashMap<>();
        int index=0;
        System.out.println("index "+index);
        String password=str[index];
        String passCrypted = encrypter.encrypt(password);
        byte[] bytepassCrypted = encrypter.getbyteCrypted(password);
        
        String passwordfounded = "";
        long initTimeSequential = 0;
        long TimeSequential=0;
        for (int it = 0; it < test; it++) {
            initTimeSequential = System.nanoTime();
            for (int j = 0; j < str.length; j++) {
                if (passCrypted.equals(encrypter.encrypt(str[j]))) {
                    passwordfounded = str[j];
                    System.out.println(passwordfounded);
                    break;

                }

            }
            long endTimeSequential = System.nanoTime();
            TimeSequential += (endTimeSequential - initTimeSequential);
            System.out.println("ciao " + String.valueOf(endTimeSequential - initTimeSequential));
        }
        
        TimeSequential/=test;
        System.out.println("TimeSequential is " + String.valueOf(TimeSequential));
        
        for (int i = 0; i < Threads.length; i++) {
            long TimeParallel = 0;
            for (int it = 0; it < test; it++) {
                ExecutorService esecutore = Executors.newFixedThreadPool(Threads[i]);

                AtomicInteger number = new AtomicInteger(1);
                AtomicLong timeout = new AtomicLong();
                AtomicBoolean passwordcheck = new AtomicBoolean(false);

                long initTimeParallel = System.nanoTime();

                for (int j = 0; j < Threads[i]; j++) {
                    esecutore.submit(new DesEncrypterThread("" + j + "", Threads[i], passCrypted, number, timeout, passwordcheck, k, bytepassCrypted));
                }
                esecutore.shutdown();
                try {
                    esecutore.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                    long endTimeParallel = System.nanoTime();
                    TimeParallel += (endTimeParallel - initTimeParallel);
                    System.out.println("ciao " + String.valueOf(endTimeParallel - initTimeParallel));
                    System.out.println("ciao " + String.valueOf(timeout.get() - initTimeParallel));
                    Thread.sleep(2000);
                } catch (InterruptedException e) {

                }
            }
            TimeParallel/=test;
            
            //myWriter.write(String.valueOf((double) ((double) TimeSequential / (double) TimeParallel))+"\n");
            //myWriter.flush();
            System.out.println(String.valueOf((double) ((double) TimeSequential / (double) TimeParallel)));
            ExecutionTime.put(String.valueOf(Threads[i]), String.valueOf((double) ((double) TimeSequential / (double) TimeParallel)));
            System.out.println("Speedup for "+Threads[i]+" threads is: "+ (double) ((double) TimeSequential / (double) TimeParallel));
        }
        //myWriter.close();
        return ExecutionTime;
    }
    
    public static HashMap passwordcryptparallelRandom(int [] num, String [] s, SecretKey k, DesEncrypter encrypter) throws Exception{
        String [] str=s;
        int [] Threads=num;
        int test = 200;
        FileWriter myWriter = new FileWriter("src/passwordcrypt/value_speedup_random_password2.txt");
        HashMap<String, String> ExecutionTime=new HashMap<>();
        long TimeSequential=0;
        int index[]=new int[test];
        for (int iterazioni = 0; iterazioni < test; iterazioni++) {

            index[iterazioni] = (int) (Math.random() * 100000000) % str.length;
            System.out.println("index["+iterazioni+"]: " + index[iterazioni]);
            String password = str[index[iterazioni]];
            String passCrypted = encrypter.encrypt(password);
            byte[] bytepassCrypted = encrypter.getbyteCrypted(password);

            String passwordfounded = "";
            long initTimeSequential = System.nanoTime();
            for (int j = 0; j < str.length; j++) {
                if (passCrypted.equals(encrypter.encrypt(str[j]))) {
                    passwordfounded = str[j];
                    System.out.println(passwordfounded);
                    break;

                }

            }
            long endTimeSequential = System.nanoTime();
            TimeSequential+=(endTimeSequential - initTimeSequential);
        System.out.println("ciao " + String.valueOf(endTimeSequential - initTimeSequential));
        }
        TimeSequential/=test;
        System.out.println("TimeSequential mean: "+TimeSequential);
        
        
        for (int i = 0; i < Threads.length; i++) {
            long TimeParallel = 0;
            
            for (int iterazioni = 0; iterazioni < test; iterazioni++) {
                AtomicInteger number = new AtomicInteger(1);
                AtomicLong timeout = new AtomicLong();
                AtomicBoolean passwordcheck = new AtomicBoolean(false);
                ExecutorService esecutore = Executors.newFixedThreadPool(Threads[i]);
                String password = str[index[iterazioni]];
                String passCrypted = encrypter.encrypt(password);
                byte[] bytepassCrypted = encrypter.getbyteCrypted(password);
                DesEncrypterThread.setdataset(str);
                long initTimeParallel = System.nanoTime();
                for (int j = 0; j < Threads[i]; j++) {
                    esecutore.submit(new DesEncrypterThread("" + j + "", Threads[i], passCrypted, number, timeout, passwordcheck, k, bytepassCrypted));
                }
                esecutore.shutdown();
                try {
                    esecutore.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                    long endTimeParallel = System.nanoTime();
                    TimeParallel += (endTimeParallel - initTimeParallel);
                    System.out.println("ciao " + String.valueOf(endTimeParallel - initTimeParallel));
                    System.out.println("ciao " + String.valueOf(timeout.get() - initTimeParallel));
                    Thread.sleep(2000);
                } catch (InterruptedException e) {

                }

            }
            TimeParallel/=test;
            //myWriter.write(String.valueOf((double) ((double) TimeSequential / (double) TimeParallel))+"\n");
            //myWriter.flush();
            System.out.println("speedup Threads "+i+" : "+String.valueOf((double) ((double) TimeSequential / (double) TimeParallel)));
            ExecutionTime.put(String.valueOf(Threads[i]), String.valueOf((double) ((double) TimeSequential / (double) TimeParallel)));
        }
        //myWriter.close();
        return ExecutionTime;
    }
    
    public static void main(String[] args) throws Exception {
        File file = new File("src/passwordcrypt/dataset_password.txt");
        FileReader fr = new FileReader(file);
        String strLine = "";
        List<String> list = new ArrayList<String>();
        BufferedReader br = new BufferedReader(fr);
        while (strLine != null)
        {
            strLine = br.readLine();
            if (strLine==null)
                break;
            list.add(strLine);
        }
        /*
        File file2 = new File("src/passwordcrypt/dataset_password.txt");
        FileReader fr2 = new FileReader(file2);
        BufferedReader br2 = new BufferedReader(fr2);
        String strLine2 = "";
        while (strLine2 != null)
        {
            strLine2 = br2.readLine();
            if (strLine2==null)
                break;
            list.add(strLine2);
        }
        */
        String []str= new String[list.size()];
        Iterator <String> iter = list.iterator();
        int i=0;
        while(iter.hasNext()){
            str[i]=iter.next();
            i+=1;
        }
        br.close();
        SecretKey key = KeyGenerator.getInstance("DES").generateKey();
        System.out.println(key);
        DesEncrypter encrypter = new DesEncrypter(key);
        String s = encrypter.encrypt(str[0]);
        
        String r = encrypter.decrypt(s);
        System.out.println(s + " " +r);
        int ThreadPoolSize= Runtime.getRuntime().availableProcessors()*2;
        System.err.println("ThreadPoolSize: "+ThreadPoolSize);
        int[]num={2,4,8,16};
        HashMap<String,String>executiontime=passwordcryptparallel(num, str, key, encrypter);
        /*
        int numThreads=4;
        ExecutorService esecutore = Executors.newFixedThreadPool(numThreads);
        
        AtomicInteger num= new AtomicInteger(1);
        AtomicLong timeout= new AtomicLong();
        AtomicBoolean passwordcheck=new AtomicBoolean(false);
        long it=System.currentTimeMillis();
        
        String passCrypted=encrypter.encrypt(str[str.length-1]);
        byte[] bytepassCrypted=encrypter.getbyteCrypted(str[str.length-1]);
        System.out.println(it-System.currentTimeMillis());
        long initTime=System.currentTimeMillis();
        for (i = 0; i < numThreads; i++) {
            esecutore.submit(new DesEncrypterThread("" + i + "", str, numThreads, passCrypted, num, timeout, passwordcheck, key, bytepassCrypted));
        }
        esecutore.shutdown();
        try {
            esecutore.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            long endTime=System.currentTimeMillis();
            System.out.println("ciao "+String.valueOf(endTime-initTime));
            System.out.println("ciao "+String.valueOf(timeout.get()-initTime));
        } catch (InterruptedException e) {
            
        }*/

        
    }
    
    

}
