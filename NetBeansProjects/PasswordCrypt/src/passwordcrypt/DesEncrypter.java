/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package passwordcrypt;

/**
 *
 * @author bernardo
 */
import com.sun.xml.internal.messaging.saaj.packaging.mime.util.BASE64DecoderStream;
import com.sun.xml.internal.messaging.saaj.packaging.mime.util.BASE64EncoderStream;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.*;
import sun.misc.BASE64Encoder;
import org.apache.commons.codec.binary.Base64;

class DesEncrypter {
  Cipher ecipher;

  Cipher dcipher;
   

  DesEncrypter(SecretKey key) throws Exception {
    ecipher = Cipher.getInstance("DES");
    dcipher = Cipher.getInstance("DES");
    ecipher.init(Cipher.ENCRYPT_MODE, key);
    dcipher.init(Cipher.DECRYPT_MODE, key);
  }

  public String encrypt(String str) throws Exception {
    // Encode the string into bytes using utf-8
    byte[] utf8 = str.getBytes("UTF8");
    // Encrypt
    byte[] enc = ecipher.doFinal(utf8);
    
    enc = BASE64EncoderStream.encode(enc);

    // Encode bytes to base64 to get a string
    return new String(enc);
  }

  public String decrypt(String str) throws Exception {
    // Decode base64 to get bytes
    byte[] dec = BASE64DecoderStream.decode(str.getBytes());

    byte[] utf8 = dcipher.doFinal(dec);

    // Decode using utf-8
    return new String(utf8, "UTF8");
  }
  
  public byte[] getbyteCrypted(String str) throws Exception {
    // Encode the string into bytes using utf-8
    byte[] utf8 = str.getBytes("UTF8");
    // Encrypt
    byte[] enc = ecipher.doFinal(utf8);

    // Encode bytes to base64 to get a string
    return enc;
  }
  
  public static void main(String[] args) throws Exception {
        File file = new File("src/passwordcrypt/dataset_password.txt");
        FileReader fr = new FileReader(file);
        //StringBuilder sb = new StringBuilder();
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
        }*/
        
        String str[]= new String[list.size()];
        Iterator <String> iter = list.iterator();
        int i=0;
        while(iter.hasNext()){
            str[i]=iter.next();
            i+=1;
        }
        System.out.println("str lenght "+str.length);
        br.close();
        SecretKey key = KeyGenerator.getInstance("DES").generateKey();
        System.out.println(key);
        DesEncrypter encrypter = new DesEncrypter(key);
        String s[]=Arrays.copyOfRange(str, (int)((0.0/2)*str.length), (int)((1.0/2)*str.length-1));
        //System.out.println(str[3]);
        //System.out.println(s[3]);
        String a="ciao1";
        String b="ciao";
        byte[] c=encrypter.getbyteCrypted(a);
        byte[] d=encrypter.getbyteCrypted(b);
        if(Arrays.equals(c, d)){
            System.out.println("yes");
        }else{
            System.out.println("no");
        }
        String firstPassword=encrypter.encrypt(str[0]);
        firstPassword=encrypter.decrypt(firstPassword);
        System.out.println(str[0]+" "+firstPassword);
        String secondPassword=encrypter.encrypt(str[(str.length/2)+1]);
        System.out.println(secondPassword);
        String thirdPassword=encrypter.encrypt(str[str.length-1]);
        System.out.println(thirdPassword);
        String password="";
        byte[] utf8byte=encrypter.getbyteCrypted(str[str.length-1]);
        long initTime=System.currentTimeMillis();
        for(int j=0; j<str.length; j++ ){
            if(Arrays.equals(utf8byte, encrypter.getbyteCrypted(str[j]))){
                password=str[j];
                System.out.println(password);
                break;
                    
            }
            
        }
        long endTime=System.currentTimeMillis();
            System.out.println("ciao "+String.valueOf(endTime-initTime));
            
        
        
       
        // TODO code application logic here
    }
    
}




