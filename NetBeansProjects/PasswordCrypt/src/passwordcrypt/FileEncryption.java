/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package passwordcrypt;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Random;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
/**
 *
 * @author bernardo
 */
public class FileEncryption {
    
    public static void main(String[] args) throws Exception {

		FileInputStream inFile = new FileInputStream("src/passwordcrypt/dataset_password.txt");
		FileOutputStream outFile = new FileOutputStream("plainfile.des");

		String password = "javapapers";
		PBEKeySpec pbeKeySpec = new PBEKeySpec(password.toCharArray());
		SecretKeyFactory secretKeyFactory = SecretKeyFactory
				.getInstance("DES");
		SecretKey secretKey = secretKeyFactory.generateSecret(pbeKeySpec);

		byte[] salt = new byte[8];
		Random random = new Random();
		random.nextBytes(salt);

		PBEParameterSpec pbeParameterSpec = new PBEParameterSpec(salt, 100);
		
                Cipher cipher = Cipher.getInstance("DES");
		
                cipher.init(Cipher.ENCRYPT_MODE, secretKey, pbeParameterSpec);
		
                outFile.write(salt);

		byte[] input = new byte[64];
		int bytesRead;
		while ((bytesRead = inFile.read(input)) != -1) {
			byte[] output = cipher.update(input, 0, bytesRead);
			if (output != null)
				outFile.write(output);
		}

		byte[] output = cipher.doFinal();
		if (output != null)
			outFile.write(output);

		inFile.close();
		outFile.flush();
		outFile.close();
	}
    
}
