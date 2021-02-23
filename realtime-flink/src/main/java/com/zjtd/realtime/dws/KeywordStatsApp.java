package com.zjtd.realtime.dws;

import com.zjtd.realtime.bean.KeywordStats;
import com.zjtd.realtime.common.GmallConstant;
import com.zjtd.realtime.dws.func.KeywordProductC2RUDTF;
import com.zjtd.realtime.dws.func.KeywordUDTF;
import com.zjtd.realtime.util.ClickhouseUtil;
import com.zjtd.realtime.util.MyKafkaUtil;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 11:26
 * @Version 1.0
 */
public class KeywordStatsApp {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String groupId = "keyword_stats_app";



        String pageViewSourceTopic ="DWD_PAGE_LOG";
        String productStatsSourceTopic ="DWS_PRODUCT_STATS";

        tableEnv.createTemporarySystemFunction("ik_analyze",  KeywordUDTF.class);
        tableEnv.createTemporarySystemFunction("keywordProductC2R",  KeywordProductC2RUDTF.class);


        // 用户的跳出
        tableEnv.executeSql("CREATE TABLE page_view (common MAP<STRING,STRING>,  page  MAP<STRING,STRING>,ts BIGINT,    rowtime  AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000  , 'yyyy-MM-dd HH:mm:ss'))    ,WATERMARK FOR  rowtime  AS rowtime) WITH ("+ MyKafkaUtil.getKafkaDDL(pageViewSourceTopic,groupId)+")");

        Table fullwordView = tableEnv.sqlQuery("select page['item'] fullword ,rowtime from page_view  where page['page_id']='good_list' and page['item'] IS NOT NULL ");

        Table keywordView = tableEnv.sqlQuery("select keyword,rowtime  from " + fullwordView + " , LATERAL TABLE(ik_analyze(fullword)) as T(keyword)");

        Table keywordStatsSearch  = tableEnv.sqlQuery(" select  keyword ,  count(*) ct, '" + GmallConstant.KEYWORD_SEARCH + "' source ,DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt   , UNIX_TIMESTAMP()*1000 ts from   "+keywordView
                + " GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ),keyword   ");

        DataStream<KeywordStats> keywordStatsSearchDataStream = tableEnv.<KeywordStats>toAppendStream(keywordStatsSearch, KeywordStats.class);
        keywordStatsSearchDataStream.print();
        keywordStatsSearchDataStream.addSink(ClickhouseUtil.<KeywordStats>getJdbcSink("insert into keyword_stats(keyword,ct,source,stt,edt,ts)   values(?,?,?,?,?,?)"));

        tableEnv.executeSql("CREATE TABLE product_stats (spu_name STRING, click_ct BIGINT,cart_ct BIGINT,order_sku_num BIGINT ,stt STRING,edt STRING )   WITH ("+ MyKafkaUtil.getKafkaDDL(productStatsSourceTopic,groupId)+")");

        Table keywordStatsProduct = tableEnv.sqlQuery("select keyword,ct,source,  stt,edt, UNIX_TIMESTAMP()*1000 ts from product_stats  , LATERAL TABLE(ik_analyze(spu_name)) as T(keyword) ,LATERAL TABLE(keywordProductC2R( click_ct ,cart_ct,order_sku_num)) as T2(ct,source)");


        DataStream<KeywordStats> keywordStatsProductDataStream = tableEnv.<KeywordStats>toAppendStream(keywordStatsProduct, KeywordStats.class);

        keywordStatsProductDataStream.print();

        keywordStatsProductDataStream.addSink(ClickhouseUtil.<KeywordStats>getJdbcSink("insert into keyword_stats(keyword,ct,source,stt,edt,ts)  values(?,?,?,?,?,?)"));


//

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
