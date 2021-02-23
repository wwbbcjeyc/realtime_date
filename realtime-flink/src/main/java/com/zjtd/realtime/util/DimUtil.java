package com.zjtd.realtime.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @Author Wang wenbo
 * @Date 2021/2/20 16:46
 * @Version 1.0
 */


@Slf4j
public class DimUtil {

    public static JSONObject  getDimInfo( String tableName, String id ){
        Tuple2<String, String> kv = Tuple2.of("id", id);
        return getDimInfo(   tableName,  kv);
    }



    public static JSONObject  getDimInfo(String tableName, Tuple2<String,String>... colNameAndValue ){
        try {
            String wheresql = " where ";
            String redisKey = "";
            for (int i = 0; i < colNameAndValue.length; i++) {
                Tuple2<String, String> nameValueTuple = colNameAndValue[i];
                String fieldName = nameValueTuple.f0;
                String fieldValue = nameValueTuple.f1;
                if (i > 0) {
                    wheresql += " and ";
                    redisKey += "_";
                }
                wheresql += fieldName + "='" + fieldValue + "'";
                redisKey += fieldValue;
            }
            JSONObject dimInfo=null;
            String dimJson = null;
            Jedis jedis = null;
            String key = "dim:" + tableName + ":" + redisKey;
            try {
                jedis = RedisUtil.getJedis();

                dimJson = jedis.get(key);
            } catch (Exception e) {
                System.out.println("缓存异常！");
                e.printStackTrace();
            }

            String sql = null;
            if (dimJson != null) {
                dimInfo = JSON.parseObject(dimJson);
            } else {
                sql = "select * from " + tableName + wheresql;
                System.out.println("查询维度sql ：" + sql);
                List<JSONObject> objectList = PhoenixUtil.queryList(sql, JSONObject.class);
                if(objectList.size()>0){
                    dimInfo = objectList.get(0);
                    if (jedis != null) {
                        jedis.setex(key, 3600 * 24, dimInfo.toJSONString());
                    }
                }  else {
                    System.out.println("维度数据未找到：" + sql);
                }
            }
            System.out.println("准备关闭连接 ");
            if (jedis != null) {
                jedis.close();
                System.out.println("关闭连接 ");
            }
            return dimInfo;
        }catch (Exception e){
            System.out.println(e.getMessage());
            throw new RuntimeException();
        }

    }




}
