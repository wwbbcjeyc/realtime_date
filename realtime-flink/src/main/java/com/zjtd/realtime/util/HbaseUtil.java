package com.zjtd.realtime.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * @Author Wang wenbo
 * @Date 2021/2/20 16:47
 * @Version 1.0
 */
public class HbaseUtil {

    private static Connection connection= null;
    public static void main(String[] args) {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hdp1:2181,hdp2:2181,hdp3:2181" );
//        conf.set("hbase.client.ipc.pool.type",...);
//        conf.set("hbase.client.ipc.pool.size",...);
        Connection connection= null;
        Table customer0317=null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            customer0317 = connection.getTable(TableName.valueOf("customer0317_his"));
            Result result = customer0317.get(new Get(Bytes.toBytes("0101")));
            byte[] value = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
            System.out.println(Bytes.toString(value));
            connection.close();

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("连接zk - hbase失败");
        }


    }

    public static  void put(String tableName,String familyName,String colName,String rowkey,String value){
        Table table = null;
        try {
            if(connection==null) open();
            table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowkey));
            table.put(new Put(Bytes.toBytes(rowkey)).addColumn(Bytes.toBytes(familyName),Bytes.toBytes(colName),Bytes.toBytes(value)) );

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("未查询出数据");
        }

    }

    public static  void put(String tableName, String familyName, String rowkey, Map<String,String> colValueMap){
        Table table = null;
        try {
            if(connection==null) open();
            table = connection.getTable(TableName.valueOf(tableName));
            List<Put> puts=new ArrayList<>();
            for (Map.Entry<String,String> entry :  colValueMap.entrySet()) {
                String colName = entry.getKey();
                String value = entry.getKey();
                puts.add(new Put(Bytes.toBytes(rowkey)).addColumn(Bytes.toBytes(familyName),Bytes.toBytes(colName),Bytes.toBytes(value)));
            }
            table.put(puts );
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("写入数据失败");
        }
    }


    public static  void putBatch(String tableName, String familyName, Map<String,Map<String,String>>  colValueMapList){
        Table table = null;
        try {
            if(connection==null) open();
            table = connection.getTable(TableName.valueOf(tableName));
            List<Put> puts=new ArrayList<>();
            for (Map.Entry<String,Map<String,String>> entry :  colValueMapList.entrySet()) {
                String rowkey = entry.getKey();
                Map<String,String> colValueMap = entry.getValue();
                for (Map.Entry<String, String> colValueEntry : colValueMap.entrySet()) {
                    String colName = colValueEntry.getKey();
                    String value = colValueEntry.getKey();
                    puts.add(new Put(Bytes.toBytes(rowkey)).addColumn(Bytes.toBytes(familyName),Bytes.toBytes(colName),Bytes.toBytes(value)));
                }
            }
            table.put(puts );
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("写入数据失败");
        }
    }


    public static  String get(String tableName,String familyName,String colName,String rowkey){
        Table table = null;
        try {
            if(connection==null) open();
            table = connection.getTable(TableName.valueOf(tableName));
            Result result = table.get(new Get(Bytes.toBytes(rowkey)));

            byte[] value = result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(colName));
            return   Bytes.toString(value) ;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("未查询出数据");
        }

    }

    public static  Map<String,String> getRow(String tableName, String familyName, String[] colNames, String rowkey){
        Table table = null;
        try {
            if(connection==null) open();
            table = connection.getTable(TableName.valueOf(tableName));
            Result result = table.get(new Get(Bytes.toBytes(rowkey)));
            Map<String,String>  resultMap=new HashMap<>();
            for (String colName : colNames) {
                byte[] value = result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(colName));
                resultMap.put(colName,Bytes.toString(value));
            }
            return    resultMap;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("未查询出数据");
        }

    }

    private  static void open(){
        if(connection==null) {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "hdp1:2181,hdp2:2181,hdp3:2181");
//        conf.set("hbase.client.ipc.pool.type",...);
//        conf.set("hbase.client.ipc.pool.size",...);

            Table customer0317 = null;
            try {
                connection = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("连接zk - hbase失败");
            }
        }
    }

}
