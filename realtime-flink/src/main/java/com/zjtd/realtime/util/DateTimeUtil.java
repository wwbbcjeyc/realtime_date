package com.zjtd.realtime.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author Wang wenbo
 * @Date 2021/2/20 16:45
 * @Version 1.0
 */
public class DateTimeUtil {

    public static SimpleDateFormat formator=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public  static String  toYMDhms(Date date){
        return formator.format(date);
    }

    public static synchronized    Long toTs(String YmDHms){
        try {
            // System.out.println("|"+YmDHms+"|");

            long time = formator.parse(YmDHms).getTime();

            return time;
        } catch ( Exception e) {

            e.printStackTrace();
            throw new RuntimeException("日期格式转换失败："+YmDHms);
        }
    }

    public static void main(String[] args) {
        System.out.println(toTs("2020-10-21 22:13:28"));
    }
}
