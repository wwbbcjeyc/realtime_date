package com.zjtd.realtime.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zjtd.realtime.bean.TableProcess;
import com.zjtd.realtime.dwd.func.DimSink;
import com.zjtd.realtime.dwd.func.TableProcessFunction;
import com.zjtd.realtime.util.MyKafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 10:41
 * @Version 1.0
 */
@Slf4j
public class BaseDBApp {

    public static void main(String[] args) {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        String groupId = "ods_order_detail_group";
        String topic = "ODS_BASE_DB_M";
        FlinkKafkaConsumer<String> source  = MyKafkaUtil.getKafkaSource(topic,groupId);

        DataStream<String> jsonDstream   = env.addSource(source);
        jsonDstream.print("data json:::::::");
        DataStream<JSONObject>  dataStream   = jsonDstream.map(jsonString -> JSON.parseObject(jsonString));
        SingleOutputStreamOperator<JSONObject> filteredDstream = dataStream.filter(jsonObject -> {
            boolean flag = jsonObject.getString("table") != null && jsonObject.getString("data") != null && jsonObject.getString("data").length() > 3;
            return flag;
        }) ;
        filteredDstream.print("json::::::::");


        final OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE){} ;


        SingleOutputStreamOperator<JSONObject> kafkaStream = filteredDstream.process(new TableProcessFunction(hbaseTag)  ) ;

        kafkaStream.getSideOutput(hbaseTag).print("hbase::::");
        kafkaStream.print("kafka ::::");
        DataStream<JSONObject> hbaseStream = kafkaStream.getSideOutput(hbaseTag);

        FlinkKafkaProducer kafkaSink   = MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>(){
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("启动 kafka sink");
            }

            /*
            从每条数据的得到该条数据应送往的主题名
             */
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                String topic =  jsonObject.getString("sink_table").toUpperCase();
                JSONObject dataJsonObj = jsonObject.getJSONObject("data");
                return new ProducerRecord(topic, dataJsonObj.toJSONString().getBytes());
            }

        });

        kafkaStream.addSink(kafkaSink);
        hbaseStream.addSink(new DimSink());


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("异常");
        }
    }

}
