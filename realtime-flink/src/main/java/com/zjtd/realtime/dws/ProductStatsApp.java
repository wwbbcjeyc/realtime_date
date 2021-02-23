package com.zjtd.realtime.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zjtd.realtime.bean.OrderWide;
import com.zjtd.realtime.bean.PaymentWide;
import com.zjtd.realtime.bean.ProductStats;
import com.zjtd.realtime.bean.UserJumpStats;
import com.zjtd.realtime.dws.func.SkuDimAsyncFunction;
import com.zjtd.realtime.util.ClickhouseUtil;
import com.zjtd.realtime.util.DateTimeUtil;
import com.zjtd.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
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
import java.util.concurrent.TimeUnit;

/**
 *  目标： 形成 以商品为准的 统计  曝光 点击  购物车  下单 支付  退单  评论数
 */

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 15:11
 * @Version 1.0
 */
public class ProductStatsApp {


    public static void main(String[] args) {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();



        String groupId="product_stats_app";

        String pageViewSourceTopic ="DWD_PAGE_LOG";
        String orderWideSourceTopic ="DWM_ORDER_WIDE";
        String paymentWideSourceTopic ="DWM_PAYMENT_WIDE";
        String cartInfoSourceTopic ="DWD_CART_INFO";
        String refundInfoSourceTopic ="DWD_ORDER_REFUND_INFO";
        String commentInfoSourceTopic ="DWD_COMMENT_INFO";

        String  productStatsSinkTopic ="DWS_PRODUCT_STATS";



        FlinkKafkaConsumer<String> pageViewSource  = MyKafkaUtil.getKafkaSource(pageViewSourceTopic,groupId);
        FlinkKafkaConsumer<String> orderWideSource  = MyKafkaUtil.getKafkaSource(orderWideSourceTopic,groupId);
        FlinkKafkaConsumer<String> paymentWideSource  = MyKafkaUtil.getKafkaSource(paymentWideSourceTopic,groupId);
        FlinkKafkaConsumer<String> cartInfoSource  = MyKafkaUtil.getKafkaSource(cartInfoSourceTopic,groupId);
        FlinkKafkaConsumer<String> refundInfoSource  = MyKafkaUtil.getKafkaSource(refundInfoSourceTopic,groupId);
        FlinkKafkaConsumer<String> commentInfoSource  = MyKafkaUtil.getKafkaSource(commentInfoSourceTopic,groupId);

        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> orderWideDStream= env.addSource(orderWideSource);
        DataStreamSource<String> paymentWideDStream= env.addSource(paymentWideSource);
        DataStreamSource<String> cartInfoDStream= env.addSource(cartInfoSource);
        DataStreamSource<String> refundInfoDStream= env.addSource(refundInfoSource);
        DataStreamSource<String> commentInfoDStream= env.addSource(commentInfoSource);

        SingleOutputStreamOperator<ProductStats> pageAndDispStatsDstream = pageViewDStream.process(new ProcessFunction<String, ProductStats>() {
            @Override
            public void processElement(String json, Context ctx, Collector<ProductStats> out) throws Exception {
                JSONObject jsonObj = JSON.parseObject(json);
                JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                String pageId = pageJsonObj.getString("page_id");
                if(pageId==null){
                    System.out.println(jsonObj);
                }
                Long ts = jsonObj.getLong("ts");
                if(pageId.equals("good_detail")){
                    Long skuId = pageJsonObj.getLong("item");
                    ProductStats productStats = ProductStats.builder().sku_id(skuId).click_ct(1L).ts(ts).build();
                    out.collect(productStats);

                }
                JSONArray displays = jsonObj.getJSONArray("display");
                if(displays!=null&&displays.size()>0){
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);
                        if(display.getString("item_type").equals("sku_id")){
                            Long skuId = display.getLong("item");
                            ProductStats productStats = ProductStats.builder().sku_id(skuId).display_ct(1L).ts(ts).build();
                            out.collect(productStats);
                        }
                    }
                }

            }
        });

        SingleOutputStreamOperator<ProductStats> orderWideStatsDstream = orderWideDStream.map(json -> {
            OrderWide orderWide = JSON.parseObject(json, OrderWide.class);
            System.out.println("orderWide:==="+orderWide);
            String create_time = orderWide.getCreate_time();
            Long ts = DateTimeUtil.toTs(create_time);
            return ProductStats.builder().sku_id(orderWide.getSku_id()).orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id()))).order_sku_num(orderWide.getSku_num()).order_amount(orderWide.getSplit_total_amount()).ts(ts).build();
        });

        SingleOutputStreamOperator<ProductStats> cartStatsDstream = cartInfoDStream.map(json -> {
            JSONObject cartInfo = JSON.parseObject(json );
            Long ts = DateTimeUtil.toTs(cartInfo.getString("create_time"));
            return ProductStats.builder().sku_id(cartInfo.getLong("sku_id") ).cart_ct(1L).ts(ts).build();
        });

        SingleOutputStreamOperator<ProductStats> paymentStatsDstream = paymentWideDStream.map(json -> {
            PaymentWide paymentWide = JSON.parseObject(json, PaymentWide.class);
            Long ts = DateTimeUtil.toTs(paymentWide.getPayment_time());
            return ProductStats.builder().sku_id(paymentWide.getSku_id()).payment_amount(paymentWide.getFinal_detail_amount()).ts(ts).build();
        });

        SingleOutputStreamOperator<ProductStats> refundStatsDstream = refundInfoDStream.map(json -> {
            JSONObject refundJsonObj = JSON.parseObject(json );
            Long ts = DateTimeUtil.toTs(refundJsonObj.getString("create_time"));
            ProductStats productStats = ProductStats.builder().sku_id(refundJsonObj.getLong("sku_id")).refund_amount(refundJsonObj.getBigDecimal("refund_amount")).refund_ct(refundJsonObj.getLong("refund_num")).ts(ts).build();
            return productStats;

        });


        SingleOutputStreamOperator<ProductStats> commonInfoStatsDstream = commentInfoDStream.map(json -> {
            JSONObject commonJsonObj = JSON.parseObject(json );
            Long ts = DateTimeUtil.toTs(commonJsonObj.getString("create_time"));
            Long goodCt=  "1201".equals(commonJsonObj.getString("appraise"))?1L:0L ;
            ProductStats productStats = ProductStats.builder().sku_id(commonJsonObj.getLong("sku_id")).comment_ct(1L).good_comment_ct(goodCt).ts(ts).build();
            return productStats;
        });


        DataStream<ProductStats> productStatDetailDStream = pageAndDispStatsDstream.union(orderWideStatsDstream, cartStatsDstream, paymentStatsDstream, refundStatsDstream, commonInfoStatsDstream);

        productStatDetailDStream.print("after union:");

        SingleOutputStreamOperator<ProductStats> productStatsWithTsStream = productStatDetailDStream.assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
            @Override
            public long extractTimestamp(ProductStats  productStats, long recordTimestamp) {
                return productStats.getTs();
            }
        }));


        SingleOutputStreamOperator<ProductStats> productStatsDstream = productStatsWithTsStream.keyBy(ProductStats::getSku_id).window(TumblingEventTimeWindows.of(Time.seconds(10))).reduce(new ReduceFunction<ProductStats>() {
            @Override
            public ProductStats reduce(ProductStats productStats1, ProductStats productStats2) throws Exception {
                //             System.out.println("p1: " + productStats1);
                //             System.out.println("p2: " + productStats2);

                productStats1.setDisplay_ct(productStats1.getDisplay_ct() + productStats2.getDisplay_ct());
                productStats1.setClick_ct(productStats1.getClick_ct() + productStats2.getClick_ct());
                productStats1.setCart_ct(productStats1.getCart_ct() + productStats2.getCart_ct());
                productStats1.setOrder_amount(productStats1.getOrder_amount().add(productStats2.getOrder_amount()));
                productStats1.getOrderIdSet().addAll(productStats2.getOrderIdSet());
                productStats1.setOrder_ct(productStats1.getOrderIdSet().size() + 0L);
                productStats1.setOrder_sku_num(productStats1.getOrder_sku_num() + productStats2.getOrder_sku_num());
                productStats1.setPayment_amount(productStats1.getPayment_amount().add(productStats2.getPayment_amount()));
                productStats1.setRefund_ct(productStats1.getRefund_ct() + productStats2.getRefund_ct());
                productStats1.setRefund_amount(productStats1.getRefund_amount().add(productStats2.getRefund_amount()));
                productStats1.setComment_ct(productStats1.getComment_ct() + productStats2.getComment_ct());
                productStats1.setGood_comment_ct(productStats1.getGood_comment_ct() + productStats2.getGood_comment_ct());

                return productStats1;
            }
        }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
            @Override
            public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> productStatsIterable, Collector<ProductStats> out) throws Exception {
                SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                for (ProductStats productStats : productStatsIterable) {
                    productStats.setStt(simpleDateFormat.format(window.getStart()));
                    productStats.setEdt(simpleDateFormat.format(window.getEnd()));
                    productStats.setTs(new Date().getTime());
                    out.collect(productStats);
                }
            }
        });


        //  productStatsDstream.print("productStatsDstream::");

        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDimStream  = AsyncDataStream.unorderedWait(productStatsDstream, new SkuDimAsyncFunction(), 20000, TimeUnit.MILLISECONDS);

        productStatsWithSkuDimStream.print();

        productStatsWithSkuDimStream.addSink(ClickhouseUtil.<UserJumpStats>getJdbcSink("insert into product_stats  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        productStatsWithSkuDimStream.map(productStats->JSON.toJSONString(productStats)).addSink(MyKafkaUtil.getKafkaSink(productStatsSinkTopic));


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
