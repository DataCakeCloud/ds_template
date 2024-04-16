package com.ushareit.data.template.utils;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import java.util.Base64;

public class EncryptUtil {

    public static final String passwdEncryptKey = "DataStudio-20210628";

    public static String encrypt(String data, String key) throws Exception {
        //创建秘钥
        DESKeySpec desKeySpec = new DESKeySpec(key.getBytes());
        SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance("DES");
        SecretKey secretKey = secretKeyFactory.generateSecret(desKeySpec);
        //加密
        Cipher cipher = Cipher.getInstance("DES/ECB/PKCS5Padding");
        cipher.init(Cipher.ENCRYPT_MODE, secretKey);
        byte[] result =  cipher.doFinal(data.getBytes());
        //使用base64进行编码
        return Base64.getEncoder().encodeToString(result);
    }

    public static String decrypt(String data, String key) throws Exception {
        //使用base64进行解码
        byte[] bs = Base64.getDecoder().decode(data);
        //创建秘钥
        DESKeySpec desKeySpec = new DESKeySpec(key.getBytes());
        SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance("DES");
        SecretKey secretKey = secretKeyFactory.generateSecret(desKeySpec);
        //解密
        Cipher cipher = Cipher.getInstance("DES/ECB/PKCS5Padding");
        cipher.init(Cipher.DECRYPT_MODE, secretKey);
        byte[] result =  cipher.doFinal(bs);
        return new String(result);
    }

    public static void main(String[] args) throws Exception {
        String data = "NnN0LifarRQco6OoqVg+TA==";
        String key = passwdEncryptKey;
        String hex = encrypt(data, key);
        System.out.println("加密之后的数据：" + hex);
        String origin = decrypt("NnN0LifarRQco6OoqVg+TA==", key);
        System.out.println("解密之后的数据:" + origin);
    }





}
