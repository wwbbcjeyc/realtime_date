package com.zjtd.realtime.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.zjtd.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import java.util.List;
import java.util.Map;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 11:24
 * @Version 1.0
 */
public class UserJumpDetailApp {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        String sourceTopic = "DWD_PAGE_LOG";
        String sinkTopic = "DWM_USER_JUMP_DETAIL";
        String groupId = "UserJumpDetailApp";

//         DataStream<String> dataStream = env
//                .fromElements(
//                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":1000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":1110}",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"detail\"},\"ts\":1800} "
//                );


        DataStreamSource<String> dataStream = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));
        dataStream.print("in json:");
        DataStream<JSONObject> jsonObjStream = dataStream.map(jsonString -> JSON.parseObject(jsonString));


        SingleOutputStreamOperator<JSONObject> jsonObjWithEtDstream = jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long recordTimestamp) {
                return jsonObject.getLong("ts");
            }
        }));

        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream = jsonObjWithEtDstream.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));


        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObj) throws Exception {
                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                System.out.println("start:"+lastPageId);
                if ( lastPageId==null||lastPageId.length()==0) {
                    return true;
                }
                return false;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObj) throws Exception {
                String pageId = jsonObj.getJSONObject("page").getString("page_id");
                System.out.println("next:"+pageId);
                if (pageId!=null&&pageId.length()>0){
                    return true;
                }
                return false;
            }
        }).within(Time.milliseconds(10000));

        PatternStream<JSONObject> patternedStream = CEP.pattern(jsonObjectStringKeyedStream, pattern);

        final OutputTag<String> timeoutTag = new OutputTag<String>("timeout"){};

//        SingleOutputStreamOperator<JSONObject> filteredStream = patternedStream
//                .flatSelect(new PatternFlatSelectFunction<JSONObject, JSONObject>() {
//                    @Override
//                    public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<JSONObject> out) throws Exception {
//                        List<JSONObject> objectList = pattern.get("start");
//                        System.out.println(objectList);
//                        for (JSONObject jsonObject : objectList) {
//                            out.collect(jsonObject);
//                        }
//                    }
//                });



        SingleOutputStreamOperator<String> filteredStream=patternedStream.flatSelect(timeoutTag, new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                        List<JSONObject> objectList = pattern.get("start");

                        for (JSONObject jsonObject : objectList) {
                            System.out.println("timeout:"+jsonObject.toJSONString());
                            out.collect(jsonObject.toJSONString());
                            System.out.println("timeout:ok:"+jsonObject.toJSONString());
                        }
                    }
                },
                new PatternFlatSelectFunction<JSONObject,String>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<String> out) throws Exception {
                        List<JSONObject> objectList = pattern.get("next");
                        for (JSONObject jsonObject : objectList) {
                            System.out.println("in time:"+jsonObject.toJSONString());
                            out.collect(jsonObject.toJSONString());
                            System.out.println("in time:"+jsonObject.toJSONString());
                        }
                    }
                });

        DataStream<String> jumpDstream = filteredStream.getSideOutput(timeoutTag);
        filteredStream.print("in time==>");
        jumpDstream.print("timeout::");

        jumpDstream.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
