package de.hpi.octopus.utils;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class Passwordcracker {

    public static String hash(String password) {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        byte[] hashedBytes = new byte[0];
        try {
            hashedBytes = digest.digest(password.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < hashedBytes.length; i++)
            stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
        return stringBuffer.toString();
    }

    public static String crack(String hash) {

        String password = null;
        for(int i = 0; i<1000000; i++) {
            password = Integer.toString(i);
            char[] leading = new char[6 - password.length()];
            Arrays.fill(leading, '0');
            password = new String(leading) + password;
            if(hash(password).equals(hash)) {
                return password;
            }
        }
        return null;
    }
}
