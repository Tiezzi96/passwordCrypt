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
import java.util.concurrent.atomic.AtomicInteger;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
/**
 *
 * @author bernardo
 */
public class DesEncrypterThread extends Thread{
    private DesEncrypter encrypter;
    private String dataset[];
    private String password;
    private String passwordcrypted;
    private byte[] utf8passwordcrypted;
    private AtomicInteger numPassword;
    private AtomicLong timeout;
    private String name;
    private AtomicBoolean passCheck;
    private int numThreads;
    
    
    public DesEncrypterThread(String name,String [] str, int threads, String password, AtomicInteger num, AtomicLong timeout, AtomicBoolean b, SecretKey key, byte []utf8password) throws  Exception{
        this.name=name;
        this.dataset=str;
        this.passwordcrypted=password;
        this.utf8passwordcrypted=utf8password;
        this.numPassword=num;
        this.encrypter=new DesEncrypter(key);
        this.timeout=timeout;
        this.passCheck=b;
        this.numThreads=threads;
    }
    
    
    public void run(){
        try{
            int inizio=(int)((Integer.valueOf(name)+0.0)/numThreads*dataset.length);
            int fine=(int)((Integer.valueOf(name)+1.0)/numThreads*dataset.length);
            for(int j=inizio; j<fine; j++){
                if(Arrays.equals(utf8passwordcrypted, encrypter.getbyteCrypted(dataset[j]))){
                    password=dataset[j];
                    numPassword.decrementAndGet();
                    System.out.println("Thread "+this.name);
                    System.out.println("Password found: "+password);
                    timeout.set(System.currentTimeMillis());
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
    
    
    public static void main(String[] args) throws Exception {
        File file = new File("src/passwordcrypt/dataset_password2.txt");
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
        File file2 = new File("src/passwordcrypt/dataset_password2.txt");
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
        int ThreadPoolSize= Runtime.getRuntime().availableProcessors()*2;
        System.err.println("ThreadPoolSize: "+ThreadPoolSize);
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
            
        }

        
    }

}
