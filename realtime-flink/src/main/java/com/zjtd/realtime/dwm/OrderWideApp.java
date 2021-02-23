package com.zjtd.realtime.dwm;

import com.alibaba.fastjson.JSON;

;
import com.zjtd.realtime.bean.*;
import com.zjtd.realtime.util.DateTimeUtil;
import com.zjtd.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 11:02
 * @Version 1.0
 */
public class OrderWideApp {

    public static void main(String[] args) {

        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

        String groupId = "order_wide_group";

        String orderInfoSourceTopic = "DWD_ORDER_INFO";
        String  orderDetailSourceTopic = "DWD_ORDER_DETAIL";

        String  orderDetailActivitySourceTopic = "DWD_ORDER_ACTIVITY_DETAIL";
        String  orderDetailCouponSourceTopic = "DWD_ORDER_DETAIL_COUPON";

        String orderWideSinkTopic = "DWM_ORDER_WIDE";

        FlinkKafkaConsumer<String> sourceOrderInfo  = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic,groupId);
        FlinkKafkaConsumer<String> sourceOrderDetail  = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic,groupId);
        FlinkKafkaConsumer<String>  orderDetailActivitySource   = MyKafkaUtil.getKafkaSource(orderDetailActivitySourceTopic,groupId);
        FlinkKafkaConsumer<String>  orderDetailCouponSource   = MyKafkaUtil.getKafkaSource(orderDetailCouponSourceTopic,groupId);

        DataStream<String> orderInfojsonDstream   = env.addSource(sourceOrderInfo);
        DataStream<String> orderDetailJsonDstream   = env.addSource(sourceOrderDetail);
        DataStream<String> orderDetailActivityJsonDstream   = env.addSource(orderDetailActivitySource);
        DataStream<String> orderDetailCouponJsonDstream   = env.addSource(orderDetailCouponSource);

        DataStream<OrderInfo>  orderInfoDStream   = orderInfojsonDstream.map(jsonString -> JSON.parseObject(jsonString,OrderInfo.class));
        DataStream<OrderDetail>  orderDetailDstream   = orderDetailJsonDstream.map(jsonString -> JSON.parseObject(jsonString, OrderDetail.class));
        DataStream<OrderDetailActivity>  orderDetailActivityDstream   = orderDetailActivityJsonDstream.map(jsonString -> JSON.parseObject(jsonString, OrderDetailActivity.class));
        DataStream<OrderDetailCoupon>  orderDetailCouponDstream   = orderDetailCouponJsonDstream.map(jsonString -> JSON.parseObject(jsonString, OrderDetailCoupon.class));



//         orderInfoDStream.print();
        orderDetailDstream.print();




        SingleOutputStreamOperator<OrderInfo> orderInfoWithEventTimeDstream = orderInfoDStream.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {

            @Override
            public long extractTimestamp(OrderInfo orderInfo, long recordTimestamp) {
                System.out.println(orderInfo.getCreate_time());
                return DateTimeUtil.toTs(orderInfo.getCreate_time());
            }
        }));

        SingleOutputStreamOperator<OrderDetail> orderDetailWithEventTimeDstream  = orderDetailDstream.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {

            @Override
            public long extractTimestamp(OrderDetail orderDetail, long recordTimestamp) {
                System.out.println(orderDetail.getCreate_time());
                return DateTimeUtil.toTs(orderDetail.getCreate_time());
            }
        }));

        KeyedStream<OrderInfo, Long>  orderInfoKeyedDstream = orderInfoWithEventTimeDstream.keyBy(orderInfo -> orderInfo.getId());
        KeyedStream<OrderDetail, Long> orderDetailKeyedStream = orderDetailWithEventTimeDstream.keyBy(orderDetail -> orderDetail.getOrder_id());

        SingleOutputStreamOperator<OrderWide> orderWideDstream = orderInfoKeyedDstream.intervalJoin(orderDetailKeyedStream).between(Time.seconds(-5), Time.seconds(5)).process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
            @Override
            public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                out.collect(new OrderWide(orderInfo, orderDetail));
            }
        });
//
        //   orderWideDstream.print();

//        SingleOutputStreamOperator<OrderWide> orderDetailAcitivityWideDstream = orderDetailActivityDstream.map(orderDetailActivity -> new OrderWide(orderDetailActivity));
//        SingleOutputStreamOperator<OrderWide> orderDetailCouponWideDstream = orderDetailCouponDstream.map(orderDetailCoupon -> new OrderWide(orderDetailCoupon));
//
//        DataStream<OrderWide> orderWideDataStream = orderWideDstream.union(orderDetailAcitivityWideDstream, orderDetailCouponWideDstream);
//
//         orderWideDataStream.print("union ::");
//        KeyedStream<OrderWide, Tuple2<Long,Long> > orderWideWithKeyDstream = orderWideDataStream.keyBy(new KeySelector<OrderWide, Tuple2<Long, Long>>() {
//            @Override
//            public Tuple2<Long, Long> getKey(OrderWide orderWide) throws Exception {
//                return   Tuple2.of(orderWide.getOrder_id(), orderWide.getDetail_id());
//            }
//        });
//        SingleOutputStreamOperator<OrderWide> orderWideJoinedDstream = orderWideWithKeyDstream.reduce(new ReduceFunction<OrderWide>() {
//            @Override
//            public OrderWide reduce(OrderWide orderWide1, OrderWide orderWide2) throws Exception {
//                orderWide1.mergeOtherOrderWide(orderWide2);
//                return orderWide1;
//            }
//        });
        //     合并购物券和 活动的维表

        orderWideDstream.print("joined ::");

        //    SingleOutputStreamOperator<OrderWide> orderWideWithActivityCouponStream  = AsyncDataStream.unorderedWait(orderWideJoinedDstream,new ActivityCouponDimAsyncFunction(),2000, TimeUnit.MILLISECONDS);

//        //  分摊优惠
//     //   SingleOutputStreamOperator<OrderWide> orderWideWithSplitAmountStream = orderWideWithActivityCouponStream.keyBy(orderWide -> orderWide.getOrder_id()).window(TumblingEventTimeWindows.of(Time.seconds(10))).process(new SplitOrderAmountFunction());
//
//        //  分摊优惠和购物券
        //    orderWideWithActivityCouponStream.print();

//        orderWideDstream.addSink(PhoenixUtil.getJdbcSink("dim_gmall_order_wide", OrderWide.class));
        orderWideDstream.map(orderWide->JSON.toJSONString(orderWide)).addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


//        orderInfoDStream.map(orderInfo->{
//            String[] cols = {"NAME","AREA_CODE","ISO_CODE","ISO_3166_2"};
//            Map<String, String> row = HbaseUtil.getRow("DIM_PROVINCE_INFO", "INFO", cols, orderInfo.getProvince_id().toString());
//            orderInfo.setProvince_name(row.get());
//            orderInfo.setProvince_area_code();
//            orderInfo.setProvince_3166_2_code();
//
//        });

//        dateStream.filter(jsonObject -> {
//            return   jsonObject.getString("table")!=null&& jsonObject.getString("data")!=null&&jsonObject.getString("data").length()>3;
//        });

    }
}
