package com.zjtd.realtime.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zjtd.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;


/**
 * @Author Wang wenbo
 * @Date 2021/2/23 11:22
 * @Version 1.0
 */
public class UniqueVisitApp {


    public static void main(String[] args) {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

        String groupId = "unique_visit_app";
        String sourceTopic = "DWD_PAGE_LOG";
        String sinkTopic = "DWM_UNIQUE_VISIT";
        FlinkKafkaConsumer<String> source  = MyKafkaUtil.getKafkaSource(sourceTopic,groupId);

        DataStreamSource<String> jsonStream = env.addSource(source);

        DataStream<JSONObject> jsonObjStream   = jsonStream.map(jsonString -> JSON.parseObject(jsonString));


        KeyedStream<JSONObject, String> jsonObjWithMidDstream = jsonObjStream.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> filteredJsonObjDstream = jsonObjWithMidDstream.filter(new RichFilterFunction<JSONObject>() {

            ValueState<String> lastVisitDateState = null;

            SimpleDateFormat simpleDateFormat = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                if (lastVisitDateState == null) {
                    ValueStateDescriptor<String> lastViewDateStateDescriptor = new ValueStateDescriptor<>("lastViewDateState", String.class);
                    lastVisitDateState = getRuntimeContext().getState(lastViewDateStateDescriptor);
                }
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                Long ts = jsonObject.getLong("ts");
                String startDate = simpleDateFormat.format(ts);
                String lastViewDate = lastVisitDateState.value();
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                if(lastPageId!=null&&lastPageId.length()>0){
                    return false;
                }
                System.out.println("起始访问");
                if (lastViewDate != null && lastViewDate.length() > 0 && startDate.equals(lastViewDate)) {
                    System.out.println("已访问：lastVisit:"+lastViewDate+"|| startDate："+startDate);
                    return false;
                } else {
                    System.out.println("未访问：lastVisit:"+lastViewDate+"|| startDate："+startDate);
                    lastVisitDateState.update(startDate);
                    return true;
                }

            }
        });


        SingleOutputStreamOperator<String> dataJsonStringDstream = filteredJsonObjDstream.map(jsonObj -> jsonObj.toJSONString());

        dataJsonStringDstream.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
