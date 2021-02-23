package com.zjtd.realtime.util;

import com.alibaba.fastjson.JSONObject;

import com.zjtd.realtime.common.GmallConfig;
import com.zjtd.realtime.common.TransientSink;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static javafx.scene.input.KeyCode.T;

/**
 * @Author Wang wenbo
 * @Date 2021/2/20 16:57
 * @Version 1.0
 */
public class PhoenixUtil {

    public static Connection conn = null;


    public static void main(String[] args) {

        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection("jdbc:phoenix:hdp1,hdp2,hdp3:2181");
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch ( Exception e) {
            e.printStackTrace();
        }

        List<JSONObject> objectList = queryList("select * from  DIM_USER_INFO", JSONObject.class);
        System.out.println(objectList);
    }


    public static  void queryInit() {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection("jdbc:phoenix:hdp1,hdp2,hdp3:2181");
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static  <T> List<T> queryList(String sql, Class<T> clazz) {
        if(conn==null){
            queryInit();
        }
        List<T> resultList = new ArrayList();
        Statement stat = null;
        try {
            stat = conn.createStatement();
            ResultSet rs = stat.executeQuery(sql);
            ResultSetMetaData md = rs.getMetaData();
            while (rs.next()) {
                T rowData = clazz.newInstance();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    BeanUtils.setProperty(rowData, md.getColumnName(i), rs.getObject(i));
                }
                resultList.add(rowData);
            }
            stat.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultList;
    }






    public static <T> SinkFunction getJdbcSink(String tableName,Class<T> clazz ){
        String sql  = genSql(clazz, tableName);

        SinkFunction<T> sink = JdbcSink.<T>sink(
                sql,
                (jdbcPreparedStatement, t) -> {
                    Field[] fields = t.getClass().getDeclaredFields();
                    for (int i = 0; i < fields.length; i++) {
                        Field field = fields[i];
                        field.setAccessible(true);
                        TransientSink transientSink = field.getAnnotation(TransientSink.class);
                        if(transientSink!=null){
                            System.out.println("跳过字段："+field.getName());
                            continue;
                        }
                        try {
                            Object o = field.get(t);

                            jdbcPreparedStatement.setString(i + 1, String.valueOf(o));
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }

                    }
                },
                new JdbcExecutionOptions.Builder().withBatchSize(2).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:phoenix:hdp1,hdp2,hdp3:2181")
                        .withDriverName("org.apache.phoenix.jdbc.PhoenixDriver")
                        .build());
        return  sink;
    }

    public static final String genSql(Class clazz,String tableName){

        StringBuilder sqlBuilder =  new StringBuilder("upsert into "+ GmallConfig.HBASE_SCHEMA+"."+tableName+" values(" )  ;
        Field[] fields = clazz.getDeclaredFields();
        for (int i = 0; i <fields.length ; i++) {
            Field field = fields[i];
            field.setAccessible(true);
            TransientSink transientSink = field.getAnnotation(TransientSink.class);
            if(transientSink!=null){
                System.out.println("跳过字段："+field.getName());
                continue;
            }
            if(i>0){
                sqlBuilder.append(",");
            }
            sqlBuilder.append("?");

        }
        sqlBuilder.append(")");
        System.out.println(sqlBuilder.toString());
        return  sqlBuilder.toString();
    }

}
