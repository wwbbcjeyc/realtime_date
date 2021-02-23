package com.zjtd.realtime.dws;


import com.zjtd.realtime.bean.PageStats;
import com.zjtd.realtime.bean.UserJumpStats;
import com.zjtd.realtime.util.ClickhouseUtil;
import com.zjtd.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 15:09
 * @Version 1.0
 */
public class PageStatsApp {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String groupId = "page_stats_app";



        String pageViewSourceTopic ="DWD_PAGE_LOG";

        // 用户的跳出
        tableEnv.executeSql("CREATE TABLE page_view (common MAP<STRING,STRING>,  page  MAP<STRING,STRING>,ts BIGINT,    rowtime  AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000  , 'yyyy-MM-dd HH:mm:ss'))    ,WATERMARK FOR  rowtime  AS rowtime) WITH ("+ MyKafkaUtil.getKafkaDDL(pageViewSourceTopic,groupId)+")");

        Table pageStatsTable = tableEnv.sqlQuery(" select TUMBLE_START(rowtime, INTERVAL '10' SECOND ) stt, TUMBLE_START(rowtime, INTERVAL '10' SECOND ) edt, page['page_id'] page_id,common['is_new'] is_new,  count(*) pv_ct,avg(cast(page['during_time'] as INT)) during_page_avg ,now() from page_view  " +
                "GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ),page['page_id'],common['is_new']    ");

        DataStream<PageStats> pageStatsDataStream = tableEnv.toAppendStream(pageStatsTable, PageStats.class);


/*
        Table midStatTable = tableEnv.sqlQuery("select DATE_FORMAT(COALESCE(pv.stt,uj.stt) ,'yyyy-MM-dd HH:mm:ss') stt, COALESCE( pv.vc,uj.vc) vc , COALESCE(pv.ch,uj.ch) ch, COALESCE(pv.ar,uj.ar) ar,COALESCE(pv.is_new,uj.is_new) is_new,COALESCE(pv_ct,0) pv_ct,COALESCE(uj_ct,0) uj_ct,COALESCE(during_page_avg,0) during_page_avg" +
                " from v_page_view pv  " +
                  " left outer join v_user_jump uj on uj.vc=pv.vc and uj.ch=pv.ch and uj.ar=pv.ar and  uj.is_new=pv.is_new and uj.stt=pv.stt  ");

        tableEnv.sqlQuery("select stt,vc,ch,ar,is_new , sum(pv_ct),uj_ct,during_page_avg")*/


        //  DataStream<UserJumpStats> userJumpStatsDataStream = tableEnv.toAppendStream(userJumpTable, UserJumpStats.class);


//        userJumpStatsDataStream.print();
//
        pageStatsDataStream.addSink(ClickhouseUtil.<UserJumpStats>getJdbcSink("insert into page_stats  values(?,?,?,?,?,?,?)"));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
