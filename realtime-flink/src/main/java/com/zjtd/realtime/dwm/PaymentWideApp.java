package com.zjtd.realtime.dwm;

import com.alibaba.fastjson.JSON;

import com.zjtd.realtime.bean.PaymentInfo;
import com.zjtd.realtime.bean.PaymentWide;
import com.zjtd.realtime.dwm.func.OrderWideAsyncFunction;
import com.zjtd.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 11:22
 * @Version 1.0
 */
public class PaymentWideApp {

    public static void main(String[] args) {

        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "DWD_PAYMENT_INFO";
        String paymentWideSinkTopic = "DWM_PAYMENT_WIDE";

        FlinkKafkaConsumer<String> paymentInfoSource  = MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic,groupId);
        DataStream<String> paymentInfojsonDstream   = env.addSource(paymentInfoSource);
        DataStream<PaymentInfo>  paymentInfoDStream   = paymentInfojsonDstream.map(jsonString -> JSON.parseObject(jsonString, PaymentInfo.class));


        SingleOutputStreamOperator<PaymentWide> paymentWideDstream = AsyncDataStream.unorderedWait(paymentInfoDStream, new OrderWideAsyncFunction(), 3000, TimeUnit.MILLISECONDS, 1000);


        paymentWideDstream.addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }




    }

}
