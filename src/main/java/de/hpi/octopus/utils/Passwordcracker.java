package com.company;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Passwordcracker {

    public  String hash(String password) {
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

    public String crack(String hash) {

        String pw = null;
        for(int i = 100000; i<=1000000; i++) {
            pw = Integer.toString(i);
            if(hash(pw).equals(hash)) {
                return pw;
            }
        }
        return null;
    }
}
