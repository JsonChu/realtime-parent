package com.moonpac.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.moonpac.realtime.common.bean.ods.KafkaVcenterInputBean;
import lombok.extern.slf4j.Slf4j;
import ognl.Ognl;
@Slf4j
public class AutoMergeOgnlUtils {

    public static boolean getBooleanValue(String expression, KafkaVcenterInputBean bean){
        try {
            Object value = Ognl.getValue(Ognl.parseExpression(expression), bean);
            return Boolean.parseBoolean(value.toString());
        }catch (Exception e){
            log.warn("计算ognl异常 getBooleanValue 表达式:{},日志数据:{}",expression, JSONObject.toJSONString(bean));
        }
        return  false;
    }

    public static String getStringValue(String expression, KafkaVcenterInputBean bean){
        try {
            return Ognl.getValue(Ognl.parseExpression(expression),bean).toString();
        }catch (Exception e){
            log.warn("计算ognl异常 getStringValue 表达式:{},日志数据:{}",expression, JSONObject.toJSONString(bean));
        }
        return null;
    }

    public static Double getDoubleValue(String expression, KafkaVcenterInputBean bean){
        try {
            Object value = Ognl.getValue(Ognl.parseExpression(expression), bean);
            return Double.parseDouble(value.toString());
        }catch (Exception e){
            log.warn("计算ognl异常 getDoubleValue 表达式:{},日志数据:{}",expression, JSONObject.toJSONString(bean));
        }
        return 0D;
    }

    public static Object getObjectValue(String expression, KafkaVcenterInputBean bean){
        try {
            return  Ognl.getValue(Ognl.parseExpression(expression),bean);
        }catch (Exception e){
            log.warn("计算ognl异常 getObjectValue 表达式:{},日志数据:{}",expression, JSONObject.toJSONString(bean));
        }
        return null;
    }

}
