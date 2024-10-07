package com.moonpac.realtime.common.util;

public class NumUtils {

    public static double doubleValue(String str) {
        try {
            return Double.parseDouble(str);
        } catch (NumberFormatException e) {
            return 0.0; // Or any default value you prefer
        }
    }
}
