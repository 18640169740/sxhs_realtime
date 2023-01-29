package com.sxhs.realtime.util;

import com.google.common.base.Strings;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;

public class AESUtil {
    /*
     * 加密用的Key 可以用26个字母和数字组成
     * 此处使用AES-128-CBC加密模式，key需要为16位。
     */

    private static String sKey = "0123456789Aacdef";
    private static String ivParameter = "0123456789Abcdef";
    private static AESUtil instance = null;

    private AESUtil() {

    }

    public static AESUtil getInstance() {
        if (instance == null) {
            instance = new AESUtil();
        }
        return instance;
    }

    // 加密
    public String encrypt(String sSrc) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        byte[] raw = sKey.getBytes();
        SecretKeySpec skeySpec = new SecretKeySpec(raw, "AES");
        IvParameterSpec iv = new IvParameterSpec(ivParameter.getBytes());
        //使用CBC模式，需要一个向量iv，可增加加密算法的强度
        cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);
        byte[] encrypted = cipher.doFinal(sSrc.getBytes(StandardCharsets.UTF_8));
        return new BASE64Encoder().encode(encrypted);
        //此处使用BASE64做转码。
    }

    // 解密
    public static String decrypt(String sSrc) throws Exception {
        try {
            byte[] raw = sKey.getBytes(StandardCharsets.US_ASCII);
            SecretKeySpec skeySpec = new SecretKeySpec(raw, "AES");
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            IvParameterSpec iv = new IvParameterSpec(ivParameter.getBytes());
            cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);
            byte[] encrypted1 = new BASE64Decoder().decodeBuffer(sSrc);
            //先用base64解密
            byte[] original = cipher.doFinal(encrypted1);
            return new String(original, StandardCharsets.UTF_8);
        } catch (Exception ex) {
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        // 需要加密的字串
        String eSrc = "a+AdeCOfBSKUACi+Yp9He3eIJ9wTFnFErSos3AEwJzU=";
        String cSrc = "奥特曼";
        System.out.println(cSrc);
        System.out.println(AESUtil.decrypt(eSrc));
        // 加密
        String enString = AESUtil.getInstance().encrypt(cSrc);
        System.out.println("加密后的字串是：" + enString);
    }

    /**
     * 对手机号码进行脱敏
     *
     * @param phoneNumber phone
     * @return phone
     */
    public static String desensitizedPhoneNumber(String phoneNumber) {
        if (phoneNumber != null) {
            phoneNumber = phoneNumber.replaceAll("(\\w{3})\\w*(\\w{4})", "$1****$2");
        }
        return phoneNumber;
    }

    /**
     * 对身份证号进行脱敏
     *
     * @param idNumber number
     * @return number
     */
    public static String desensitizedIdCard(String idNumber) {
        if (!Strings.isNullOrEmpty(idNumber)) {
            if (idNumber.length() == 15) {
                idNumber = idNumber.replaceAll("(\\w{6})\\w*(\\w{3})", "$1******$2");
            }
            if (idNumber.length() == 18) {
                idNumber = idNumber.replaceAll("(\\w{6})\\w*(\\w{3})", "$1*********$2");
            }
        }
        return idNumber;
    }

    /**
     * 对姓名进行脱敏
     *
     * @param userName name
     * @return name
     */
    public static String desensitizedUserName(String userName) {
        if (!Strings.isNullOrEmpty(userName)) {
            if (userName.length() <= 2) {
                // 隐藏第一个字
                return "*" + userName.substring(1);
            } else {
                // 隐藏前2个字段
                return "**" + userName.substring(2);
            }
        }
        return userName;
    }
}
