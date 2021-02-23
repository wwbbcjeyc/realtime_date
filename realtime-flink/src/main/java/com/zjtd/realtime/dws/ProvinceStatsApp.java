package com.zjtd.realtime.dws;

import com.alibaba.fastjson.JSON;
import com.zjtd.realtime.bean.OrderWide;
import com.zjtd.realtime.bean.ProvinceStats;
import com.zjtd.realtime.bean.UserJumpStats;
import com.zjtd.realtime.dws.func.ProvinceDimAsyncFunction;
import com.zjtd.realtime.util.ClickhouseUtil;
import com.zjtd.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import ru.ivi.opensource.flinkclickhousesink.ClickhouseSink;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 15:29
 * @Version 1.0
 */
public class ProvinceStatsApp {


    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        String groupId = "province_stats_app";

        String orderWideSourceTopic = "DWM_ORDER_WIDE";


        FlinkKafkaConsumer<String> orderWideSource  = MyKafkaUtil.getKafkaSource(orderWideSourceTopic,groupId);

        DataStreamSource<String> orderWideDataStreamSource = env.addSource(orderWideSource);

        SingleOutputStreamOperator<ProvinceStats> provinceStatsDstream = orderWideDataStreamSource.map(json -> {
            OrderWide orderWide = JSON.parseObject(json, OrderWide.class);
            if(orderWide.getSplit_total_amount()==null){
                System.out.println("11111111111111111111111111111111111---->>>"+orderWide);
            }
            return new ProvinceStats(orderWide);
        });


        provinceStatsDstream.print();


        SingleOutputStreamOperator<ProvinceStats> provinceStatsWithWatermarkDstream = provinceStatsDstream.assignTimestampsAndWatermarks(WatermarkStrategy.<ProvinceStats>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<ProvinceStats>() {
            @Override
            public long extractTimestamp(ProvinceStats provinceStats, long recordTimestamp) {
                return provinceStats.getTs();
            }
        }));
        KeyedStream<ProvinceStats, Long> provinceStatsKeyedStream = provinceStatsWithWatermarkDstream.keyBy(provinceStats -> provinceStats.getProvince_id());

        SingleOutputStreamOperator<ProvinceStats> provinceStatsReduceStream = provinceStatsKeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce((provinceStats1, provinceStats2) -> {
                    provinceStats1.getOrderIdSet().addAll(provinceStats2.getOrderIdSet());  //TODO 待优化
                    provinceStats1.setOrder_count(provinceStats1.getOrderIdSet().size()+0L);
                    provinceStats1.setOrder_amount(provinceStats1.getOrder_amount().add(provinceStats2.getOrder_amount()));
                    return provinceStats1;
                },new ProcessWindowFunction<ProvinceStats, ProvinceStats,Long, TimeWindow>() {
                    @Override
                    public void process(Long provinceId, Context context, Iterable<ProvinceStats> provinceStatsIterable, Collector<ProvinceStats> out) throws Exception {
                        SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        for (ProvinceStats provinceStats : provinceStatsIterable) {
                            String startDate =simpleDateFormat.format(new Date(context.window().getStart()));
                            String endDate = simpleDateFormat.format(new Date(context.window().getEnd()));
                            provinceStats.setStt(startDate);
                            provinceStats.setEdt(endDate);
                            out.collect(provinceStats);
                        }
                    }
                });


        SingleOutputStreamOperator<ProvinceStats> provinceStatsWithDimStream  = AsyncDataStream.unorderedWait(provinceStatsReduceStream, new ProvinceDimAsyncFunction(), 10000, TimeUnit.MILLISECONDS);
        provinceStatsWithDimStream.print();
        provinceStatsWithDimStream.addSink(ClickhouseUtil.<UserJumpStats>getJdbcSink("insert into  province_stats  values(?,?,?,?,?,?,?,?,?,?)"));



        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
