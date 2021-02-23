package com.zjtd.realtime.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import java.util.Properties;

/**
 * @Author Wang wenbo
 * @Date 2021/2/20 16:52
 * @Version 1.0
 */
public class MyKafkaUtil {
    static String kafkaServer="hdp1:9092,hdp2:9092,hdp3:9092";

    static final  String DEFAULT_TOPIC="DEFAULT_DATA";

    public static FlinkKafkaConsumer<String> getKafkaSource(String topic , String groupId ) {
        Properties prop = new Properties();
        prop.setProperty("group.id", groupId);
        prop.setProperty("bootstrap.servers",kafkaServer);
        FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer (topic,new SimpleStringSchema(),prop);
        return kafkaConsumer;
    }

    public static FlinkKafkaProducer getKafkaSink(String topic){
        return  new FlinkKafkaProducer(kafkaServer,topic,new SimpleStringSchema());
    }

    public static String getKafkaDDL(String topic,String groupId){
        String ddl="'connector' = 'kafka', " +
                " 'topic' = '"+topic+"',"   +
                " 'properties.bootstrap.servers' = '192.168.11.101:9092,192.168.11.102:9092,192.168.11.103:9092', " +
                " 'properties.group.id' = '"+groupId+ "', " +
                "  'format' = 'json', " +
                "  'scan.startup.mode' = 'latest-offset'  ";

        return  ddl;
    }



    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema( KafkaSerializationSchema<T> serializationSchema ){
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",kafkaServer);

        return new  FlinkKafkaProducer<T> (DEFAULT_TOPIC,serializationSchema,prop,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
}
