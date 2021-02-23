package com.zjtd.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import com.google.common.base.Converter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.phoenix.util.PropertiesUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author Wang wenbo
 * @Date 2021/2/20 16:56
 * @Version 1.0
 */
public class MySQLUtil {
    public static  List<JSONObject> queryList(String sql ) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            List<JSONObject> resultList  = new ArrayList<JSONObject>();
            Connection conn   = DriverManager.getConnection("jdbc:mysql://hdp1:3306/gmall0213_rs?characterEncoding=utf-8&useSSL=false","root","123123");
            Statement stat   = conn.createStatement();
            ResultSet rs   = stat.executeQuery(sql );
            ResultSetMetaData md  = rs.getMetaData();
            while (  rs.next() ) {
                JSONObject rowData = new JSONObject();
                for (int i=1;  i<= md.getColumnCount() ;i++  ) {
                    rowData.put(md.getColumnName(i), rs.getObject(i));
                }
                resultList.add(rowData);
            }

            stat.close();
            conn.close();
            return  resultList ;

        } catch ( Exception e) {
            e.printStackTrace();
            throw  new RuntimeException("查询mysql失败！");
        }
    }


    public static  <T> List<T> queryList(String sql,Class<T> clazz,Boolean underScoreToCamel ) {
        try {

            Class.forName("com.mysql.jdbc.Driver");
            List<T> resultList  = new ArrayList<T>();
            Connection conn   = DriverManager.getConnection("jdbc:mysql://hdp1:3306/gmall_realtime?characterEncoding=utf-8&useSSL=false","root","123123");
            Statement stat   = conn.createStatement();
            ResultSet rs   = stat.executeQuery(sql );
            ResultSetMetaData md  = rs.getMetaData();
            while (  rs.next() ) {

                T obj = clazz.newInstance();
                for (int i=1;  i<= md.getColumnCount() ;i++  ) {
                    String propertyName=md.getColumnName(i);
                    if(underScoreToCamel) {
                        propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,md.getColumnName(i));
                    }
                    BeanUtils.setProperty(obj,propertyName, rs.getObject(i));
                }
                resultList.add(obj);
            }

            stat.close();
            conn.close();
            return  resultList ;

        } catch ( Exception e) {
            e.printStackTrace();
            throw  new RuntimeException("查询mysql失败！");
        }
    }
}
