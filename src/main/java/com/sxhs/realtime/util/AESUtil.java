package com.sxhs.realtime.util;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

/**AES 是一种可逆加密算法，对用户的敏感信息加密处理
 * 对原始数据进行AES加密后，在进行Base64编码转化；
 */
public class AESUtil {
    /*
     * 加密用的Key 可以用26个字母和数字组成
     * 此处使用AES-128-CBC加密模式，key需要为16位。
     */
    private static String sKey="0123456789Aacdef";
    private static String ivParameter="0123456789Abcdef";
    private static AESUtil instance=null;
    private AESUtil(){

    }
    public static AESUtil getInstance(){
        if (instance==null){
            instance= new AESUtil();
        }
        return instance;
    }
    // 加密
    public static String encrypt(String sSrc) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        byte[] raw = sKey.getBytes();
        SecretKeySpec skeySpec = new SecretKeySpec(raw, "AES");
        IvParameterSpec iv = new IvParameterSpec(ivParameter.getBytes());//使用CBC模式，需要一个向量iv，可增加加密算法的强度
        cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);
        byte[] encrypted = cipher.doFinal(sSrc.getBytes("utf-8"));
        return new BASE64Encoder().encode(encrypted);//此处使用BASE64做转码。
    }

    // 解密
    public static String decrypt(String sSrc) throws Exception {
        try {
            byte[] raw = sKey.getBytes("ASCII");
            SecretKeySpec skeySpec = new SecretKeySpec(raw, "AES");
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            IvParameterSpec iv = new IvParameterSpec(ivParameter.getBytes());
            cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);
            byte[] encrypted1 = new BASE64Decoder().decodeBuffer(sSrc);//先用base64解密
            byte[] original = cipher.doFinal(encrypted1);
            String originalString = new String(original,"utf-8");
            return originalString;
        } catch (Exception ex) {
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
// 需要加密的字串
        String cSrc = "13263153721";
        System.out.println(cSrc);
// 加密
        long lStart = System.currentTimeMillis();
        AESUtil.getInstance();
        String enString = encrypt(cSrc);
        System.out.println("加密后的字串是："+ enString);
//
        long lUseTime = System.currentTimeMillis()-lStart;
        System.out.println("加密耗时："+ lUseTime + "毫秒");
//// 解密
        lStart = System.currentTimeMillis();
        AESUtil.getInstance();
        String DeString = decrypt("M9uFGSDL4QF2rXdSLeJDSC/4W+7Fvl/6/WFE8NkbuXk=");
        System.out.println("解密后的字串是" + DeString);
        lUseTime = System.currentTimeMillis()-lStart;
        System.out.println("解密耗时：" + lUseTime + "毫秒");
        AESUtil.getInstance();
        String DeString2 = decrypt("8XkGKhjsV2NxVd+ztoTOrw==");
        System.out.println("DeString2解密后的字串是" + DeString2);
        lUseTime = System.currentTimeMillis()-lStart;
        System.out.println("解密耗时：" + lUseTime + "毫秒");

    }
}

