package com.zjtd.realtime.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zjtd.realtime.bean.MidStats;
import com.zjtd.realtime.util.ClickhouseUtil;
import com.zjtd.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 15:05
 * @Version 1.0
 */

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *  要不要把多个明细的同样的维度统计在一起
 *
 * 因为单位时间内mid的操作数据非常有限不能明显的压缩数据量（如果是数据量够大，或者单位时间够长可以）
 *  所以用常用统计的四个维度进行聚合 渠道、新老用户、app版本、省市区域
 *  度量值包括 启动、日活（当日首次启动）、访问页面数、新增用户数、跳出数、平均页面停留时长、总访问时长
 *  聚合窗口： 10秒
 */
public class MidStatsApp {


    /***
     * 1  各个数据在维度聚合前不具备关联性 ，所以 先进行维度聚合
     * 2  进行关联  这是一个fulljoin
     * 3  可以考虑使用flinksql 完成
     * @param args
     */
    public static void main(String[] args) {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment( );

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();



        String groupId="mid_stat_app";




        String uniqueVisitSourceTopic ="DWM_UNIQUE_VISIT";
        String pageViewSourceTopic ="DWD_PAGE_LOG";
        String userJumpDetailSourceTopic = "DWM_USER_JUMP_DETAIL";


        FlinkKafkaConsumer<String> uniqueVisitSource  = MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic,groupId);
        FlinkKafkaConsumer<String> pageViewSource  = MyKafkaUtil.getKafkaSource(pageViewSourceTopic,groupId);
        FlinkKafkaConsumer<String> userJumpSource  = MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic,groupId);

        DataStreamSource<String> uniqueVisitDStream = env.addSource(uniqueVisitSource);
        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> userJumpDStream= env.addSource(userJumpSource);

        //  uniqueVisitDStream.print("uv=====>");

        //pageViewDStream.print("pv-------->");

        SingleOutputStreamOperator<MidStats> uniqueVisitStatsDstream = uniqueVisitDStream.map(json -> {
            JSONObject jsonObj = JSON.parseObject(json);
            return new MidStats("",  "",
                    jsonObj.getJSONObject("common").getString("vc"),
                    jsonObj.getJSONObject("common").getString("ch"),
                    jsonObj.getJSONObject("common").getString("ar"),
                    jsonObj.getJSONObject("common").getString("is_new"),
                    1L, 0L, 0L,0L, 0L, jsonObj.getLong("ts")  ,new HashSet<JSONObject>(Collections.singleton(jsonObj)));
        });

        // uniqueVisitStatsDstream.print("uv -------->");

        SingleOutputStreamOperator<MidStats> pageViewStatsDstream = pageViewDStream.map(json -> {
            //  System.out.println("pv:"+json);
            JSONObject jsonObj = JSON.parseObject(json);
            return new MidStats("","",
                    jsonObj.getJSONObject("common").getString("vc"),
                    jsonObj.getJSONObject("common").getString("ch"),
                    jsonObj.getJSONObject("common").getString("ar"),
                    jsonObj.getJSONObject("common").getString("is_new"),
                    0L, 1L, 0L,0L, jsonObj.getJSONObject("page").getLong("during_time"), jsonObj.getLong("ts") ,new HashSet<JSONObject>(Collections.singleton(jsonObj)));
        });
        // pageViewStatsDstream.print("pvpvpv::");

        SingleOutputStreamOperator<MidStats> sessionVisitDstream = pageViewDStream.process(new ProcessFunction<String, MidStats>() {
            @Override
            public void processElement(String json, Context ctx, Collector<MidStats> out) throws Exception {
                JSONObject jsonObj = JSON.parseObject(json);
                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                if (lastPageId == null||lastPageId.length()==0) {
                    //    System.out.println("sc:"+json);
                    MidStats midStats = new MidStats("", "",
                            jsonObj.getJSONObject("common").getString("vc"),
                            jsonObj.getJSONObject("common").getString("ch"),
                            jsonObj.getJSONObject("common").getString("ar"),
                            jsonObj.getJSONObject("common").getString("is_new"),
                            0L, 0L, 1L, 0L, 0L, jsonObj.getLong("ts"),new HashSet<JSONObject>(Collections.singleton(jsonObj)));
                    out.collect(midStats);
                }
            }
        });

        //  sessionVisitDstream.print("session:===>");

        SingleOutputStreamOperator<MidStats> userJumpStatDstream = userJumpDStream.map(json -> {
            JSONObject jsonObj = JSON.parseObject(json);
            return new MidStats("","",
                    jsonObj.getJSONObject("common").getString("vc"),
                    jsonObj.getJSONObject("common").getString("ch"),
                    jsonObj.getJSONObject("common").getString("ar"),
                    jsonObj.getJSONObject("common").getString("is_new"),
                    0L, 0L, 0L, 1L,0L, jsonObj.getLong("ts"),new HashSet<JSONObject>(Collections.singleton(jsonObj)) );
        });

        DataStream<MidStats> unionDetailDstream = uniqueVisitStatsDstream.union(pageViewStatsDstream,sessionVisitDstream, userJumpStatDstream);

        // unionDetailDstream.print("union ------>");

        //  SingleOutputStreamOperator<MidStats> midStatsWithWatermarkDstream = unionDetailDstream.assignTimestampsAndWatermarks(WatermarkStrategy.<MidStats>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<MidStats>() {
        SingleOutputStreamOperator<MidStats> midStatsWithWatermarkDstream = unionDetailDstream.assignTimestampsAndWatermarks(WatermarkStrategy.<MidStats>forBoundedOutOfOrderness(Duration.ofSeconds(1)) .withTimestampAssigner(new SerializableTimestampAssigner<MidStats>() {
            @Override
            public long extractTimestamp(MidStats midStats, long recordTimestamp) {
                //         System.out.println("event time:"+midStats.getTs());
                return midStats.getTs();
            }
        }));
        midStatsWithWatermarkDstream.print("after union:::");


        KeyedStream<MidStats, Tuple4<String,String,String,String>> midStatsTuple4KeyedStream = midStatsWithWatermarkDstream.<Tuple4<String,String,String,String>> keyBy(new KeySelector<MidStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(MidStats midStats) throws Exception {
                return new Tuple4<>(midStats.getVc(),midStats.getCh(),midStats.getAr(),midStats.getIs_new());
            }
        });

        WindowedStream<MidStats, Tuple4<String,String,String,String>, TimeWindow> windowStream = midStatsTuple4KeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        //  windowStream.trigger(  CountTrigger.of(1));
        SingleOutputStreamOperator<MidStats> midStatsDstream = windowStream.reduce(new ReduceFunction<MidStats>() {
            @Override
            public MidStats reduce(MidStats stats1, MidStats stats2) throws Exception {
                //   System.out.println("stats1"+stats1);
                //   System.out.println("stats12"+stats2);
                stats1.setPv_ct(stats1.getPv_ct() + stats2.getPv_ct());
                stats1.setUv_ct(stats1.getUv_ct() + stats2.getUv_ct());
                stats1.setUj_ct(stats1.getUj_ct() + stats2.getUj_ct());
                stats1.setSv_ct(stats1.getSv_ct()+stats2.getSv_ct());
                stats1.setDur_sum(stats1.getDur_sum() + stats2.getDur_sum());
                stats1.getMidStatsSet().addAll(stats2.getMidStatsSet());
                return stats1;
            }
        }, new ProcessWindowFunction<MidStats, MidStats, Tuple4<String,String,String,String>, TimeWindow>() {
            @Override
            public void process(Tuple4<String,String,String,String> tuple4, Context context, Iterable<MidStats> midStatsIn, Collector<MidStats> midStatsOut) throws Exception {
                SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                for (MidStats midStats : midStatsIn) {

                    String startDate =simpleDateFormat.format(new Date(context.window().getStart()));
                    String endDate = simpleDateFormat.format(new Date(context.window().getEnd()));

                    midStats.setStt(startDate);
                    midStats.setEdt(endDate);
                    //  System.out.println("midStats::"+midStats);
                    midStatsOut.collect(midStats);
                }
            }
        });

        // 关于开窗口 需要知道的几件事：

        //1 时间周期 用eventTime 开的10秒的窗口都是以自然时间的 秒数的10倍数 开窗口的，所以不管何时执行，任何数据都会落在固定的窗口内
        //2 一旦超越窗口范围+水位线延迟   就会触发窗口计算
        //3  full join操作 产生的是可回溯流 意味着计算结果可能会出现修改， 这就需要写入操作必须实现幂等性。  如果多流join 意味着每个表的数据到位都会产生新数据，只有最后一个到位的数据表产生的数据才是完整的join数据
        // 3.1 放弃fulljoin操作 因为fulljoin带来的是数据一定会出现大量的重复  因为ck无法保证严格的幂等性 所以会出现一定时间内数据重复
        //4  问题？ 1 数据重发  由于时间点过了水位线，所以历史数据是不会被计算的
        //          2 如果数据重复 聚合前并没有去重操作 聚合操作也不幂等所以会出现重复累加的情况。

        midStatsDstream.print();
        midStatsDstream.addSink(ClickhouseUtil.getJdbcSink("insert into mid_stats values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        // 活跃用户/////////////////////////////////////
//        tableEnv.executeSql("CREATE TABLE user_view (common MAP<STRING,STRING>, `start` MAP<STRING,STRING>,ts BIGINT,    rowtime  AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000  , 'yyyy-MM-dd HH:mm:ss'))    ,WATERMARK FOR  rowtime  AS rowtime) WITH ("+MyKafkaUtil.getKafkaDDL(uniqueVisitSourceTopic,groupId)+")");
//
//
////         tableEnv.executeSql("create view v_user_view as select TUMBLE_START(rowtime, INTERVAL '10' SECOND ) stt , common['vc'] vc,common['ch'] ch,common['ar'] ar,common['is_new'] is_new,count(*) uv_ct from user_view  " +
////                "GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ),common['vc'],common['ch'],common['ar'],common['is_new'] " );
//
////          tableEnv.toAppendStream( tableEnv.sqlQuery("select * from v_user_view "), TypeInformation.of(Row.class)).print("uv-->");
//
//        // 页面访问////////////////////////////////////
//
//        tableEnv.executeSql("CREATE TABLE page_view (common MAP<STRING,STRING>,  page  MAP<STRING,STRING>,ts BIGINT,    rowtime  AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000  , 'yyyy-MM-dd HH:mm:ss'))    ,WATERMARK FOR  rowtime  AS rowtime) WITH ("+MyKafkaUtil.getKafkaDDL(pageViewSourceTopic,groupId)+")");
//
////        tableEnv.executeSql("create view v_page_view as select TUMBLE_START(rowtime, INTERVAL '10' SECOND ) stt, common['vc'] vc,common['ch'] ch,common['ar'] ar,common['is_new'] is_new,  count(*) pv_ct,sum(cast(page['during_time'] as INT))/count(*) during_page_avg from page_view  " +
////                "GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ),common['vc'],common['ch'],common['ar'],common['is_new'] " );
//
////        tableEnv.toAppendStream( tableEnv.sqlQuery("select * from v_page_view "), TypeInformation.of(Row.class)).print("pv-->");
//
//
//        // 用户的跳出
//        tableEnv.executeSql("CREATE TABLE user_jump (common MAP<STRING,STRING>, `page` MAP<STRING,STRING>,ts BIGINT,    rowtime  AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000  , 'yyyy-MM-dd HH:mm:ss'))    ,WATERMARK FOR  rowtime  AS rowtime) WITH ("+MyKafkaUtil.getKafkaDDL(userJumpDetailSourceTopic,groupId)+")");
//
////        tableEnv.executeSql("create view v_user_jump as select TUMBLE_START(rowtime, INTERVAL '10' SECOND ) stt, common['vc'] vc,common['ch'] ch,common['ar'] ar,common['is_new'] is_new,count(*)  uj_ct from user_jump  " +
////                "GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ),common['vc'],common['ch'],common['ar'],common['is_new'] " );
//
//
//
////        Table midStatTable = tableEnv.sqlQuery("select DATE_FORMAT(COALESCE(uv.stt,pv.stt,uj.stt) ,'yyyy-MM-dd HH:mm:ss') stt, COALESCE(uv.vc,pv.vc,uj.vc) vc , COALESCE(uv.ch,pv.ch,uj.ch) ch, COALESCE(uv.ar,pv.ar,uj.ar) ar,COALESCE(uv.is_new,pv.is_new,uj.is_new) is_new, COALESCE(uv_ct,0) uv_ct,COALESCE(pv_ct,0) pv_ct,COALESCE(uj_ct,0) uj_ct,COALESCE(during_page_avg,0) during_page_avg" +
////                " from v_user_view uv  " +
////                " full join v_page_view pv on uv.vc=pv.vc and uv.ch=pv.ch and uv.ar=pv.ar and  uv.is_new=pv.is_new and uv.stt=pv.stt  " +
////                " full join v_user_jump uj on uj.vc=pv.vc and uj.ch=pv.ch and uj.ar=pv.ar and  uj.is_new=pv.is_new and uj.stt=pv.stt  ");
//
////        Table intervalMidStatTable = tableEnv.sqlQuery("select DATE_FORMAT(COALESCE(uv.stt,pv.stt,uj.stt) ,'yyyy-MM-dd HH:mm:ss') stt, COALESCE(uv.vc,pv.vc,uj.vc) vc , COALESCE(uv.ch,pv.ch,uj.ch) ch, COALESCE(uv.ar,pv.ar,uj.ar) ar,COALESCE(uv.is_new,pv.is_new,uj.is_new) is_new, COALESCE(uv_ct,0) uv_ct,COALESCE(pv_ct,0) pv_ct,COALESCE(uj_ct,0) uj_ct,COALESCE(during_page_avg,0) during_page_avg" +
////                " from v_page_view  pv " +
////                " inner  join v_user_view uv on uv.vc=pv.vc and uv.ch=pv.ch and uv.ar=pv.ar and  uv.is_new=pv.is_new and uv.stt=pv.stt  " +
////                "  inner join v_user_jump uj on uj.vc=pv.vc and uj.ch=pv.ch and uj.ar=pv.ar and  uj.is_new=pv.is_new and uj.stt=pv.stt  ");
//        String unionSql = "select DATE_FORMAT( TUMBLE_END(rowtime, INTERVAL '10' SECOND ) ,'yyyy-MM-dd HH:mm:ss') stt, vc,ch ,ar ,is_new ,sum(uv) uv_ct,sum( pv) pv_ct,sum( uj) uj_ct,  sum( during_time)/sum( pv) dur_avg from (" +
//                " select  rowtime,  common['vc'] vc,common['ch'] ch,common['ar'] ar,common['is_new'] is_new, 1 uv ,0 pv ,0 uj , 0  during_time   from  user_view  " +
//                " union all " +
//                " select  rowtime,  common['vc'] vc,common['ch'] ch,common['ar'] ar,common['is_new'] is_new , 0 uv , 1 pv , 0 uj , cast(page['during_time'] as INT)  during_time  from  page_view " +
//                " union all " +
//                " select  rowtime,  common['vc'] vc,common['ch'] ch,common['ar'] ar,common['is_new'] is_new , 0 uv , 0 pv , 1 uj , 0 during_time  from  user_jump "
//                + " ) all_stats   GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ), vc,ch ,ar ,is_new   ";
//
//        // tableEnv.executeSql(" create table  mid_stat (stt STRING,vc STRING,ch STRING , ar STRING, is_new STRING,uv_ct BIGINT,pv_ct BIGINT,uj_ct BIGINT,during_page_avg BIGINT) with ("+ ClickhouseUtil.getClickhouseDDL("mid_stat") +")");
//
//    //    tableEnv.executeSql(" insert into mid_stat select  stt ,vc, ch, ar, is_new,uv_ct,pv_ct,uj_ct,during_page_avg from "+midStatTable);
//
//        Table midStatsTable = tableEnv.sqlQuery(unionSql);
//
//        DataStream<MidStats> midStatsDataStream = tableEnv.toAppendStream(midStatsTable, MidStats.class);
//
//        midStatsDataStream.print("-->");

        // midStatsDataStream.addSink(ClickhouseUtil.<UserViewStats>getJdbcSink("insert into mid_stats values(?,?,?,?,?,?,?,?,?)"));


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }






}
