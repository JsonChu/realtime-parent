package com.moonpac.realtime.common.util;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

public class DateUtils {

    public static long convertToHourLevel(long eventTimestamp) {
        Instant instant = Instant.ofEpochSecond(eventTimestamp);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        // 转换为小时级别，将分钟和秒都置为0，并转换为Epoch时间
        return dateTime.withMinute(0).withSecond(0).atZone(ZoneId.systemDefault()).toEpochSecond();
    }

    public static String formatTimestamp(long timestamp) {
        // 创建 SimpleDateFormat 对象，指定日期时间格式
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        // 将时间戳转换为 Date 对象
        Date date = new Date(timestamp * 1000); // 将秒转换为毫秒
        // 使用 SimpleDateFormat 格式化 Date 对象为字符串
        return sdf.format(date);
    }

    /*
     获取当前时间向下取整到最近的 5 分钟的时间对应的时间戳秒值
     */
    public static long getCurrentTimeRoundedToFiveMinutesInSeconds() {
        // 获取当前时间
        LocalDateTime currentTime = LocalDateTime.now();

        // 向下取整到最近的 5 分钟
        int minute = currentTime.getMinute();
        int remainder = minute % 5;
        LocalDateTime roundedTime = currentTime.minusMinutes(remainder).withSecond(0).withNano(0);

        // 获取时间戳秒值
        return roundedTime.atZone(ZoneId.systemDefault()).toEpochSecond();
    }

    public static void main(String[] args) {
        // 获取向下取整到最近的 5 分钟的当前时间的时间戳（秒）
        long roundedTimeInSeconds = getCurrentTimeRoundedToFiveMinutesInSeconds();

        // 打印结果
        System.out.println("向下取整到最近的 5 分钟的当前时间的时间戳（秒）: " + roundedTimeInSeconds);
    }

}
