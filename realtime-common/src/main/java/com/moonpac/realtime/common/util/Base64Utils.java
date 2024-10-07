package com.moonpac.realtime.common.util;

import java.util.Base64;

public class Base64Utils {

    // Base64 编码
    public static String encode(String plainText) {
        byte[] encodedBytes = Base64.getEncoder().encode(plainText.getBytes());
        return new String(encodedBytes);
    }

    // Base64 解码
    public static String decode(String base64Text) {
        byte[] decodedBytes = Base64.getDecoder().decode(base64Text);
        return new String(decodedBytes);
    }

    public static void main(String[] args) {
        // 示例用法
        String originalText = "MP_AIOPS_VDI";

        // 编码
        String encodedText = encode(originalText);
        System.out.println("Encoded: " + encodedText);

        // 解码
        String decodedText = decode(encodedText);
        System.out.println("Decoded: " + decodedText);
    }
}
