package com.zjtd.realtime.util;

import com.zjtd.realtime.common.TransientSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;

/**
 * @Author Wang wenbo
 * @Date 2021/2/20 16:38
 * @Version 1.0
 */
public class ClickhouseUtil {


    public static String getClickhouseDDL(String table ){
        String ddl="'connector' = 'jdbc', " +
                "   'url' = 'jdbc:clickhouse://cc-uf6tj4rjbu5ez10lb.ads.aliyuncs.com:8123/ods', " +
                "  'driver' = 'ru.yandex.clickhouse.ClickHouseDriver'," +
                "   'table-name' = '"+table+"'," +
                " 'sink.buffer-flush.max-rows' = '100', " +
                " 'sink.buffer-flush.interval' = '10s' ";

        return  ddl;
    }




    public static <T> SinkFunction getJdbcSink(String sql ){
        SinkFunction<T> sink = JdbcSink.<T>sink(
                sql,
                (jdbcPreparedStatement, t) -> {
                    Field[] fields = t.getClass().getDeclaredFields();
                    int skip=0;
                    for (int i = 0; i < fields.length; i++) {
                        Field field = fields[i];
                        TransientSink transientSink = field.getAnnotation(TransientSink.class);
                        if(transientSink!=null){
                            System.out.println("跳过字段："+field.getName());
                            skip++;
                            continue;
                        }
                        field.setAccessible(true);
                        try {
                            Object o = field.get(t);
                            jdbcPreparedStatement.setObject(i-skip + 1, o);
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }

                    }
                },
                new JdbcExecutionOptions.Builder().withBatchSize(2).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://cc-uf6tj4rjbu5ez10lb.ads.aliyuncs.com:8123/ods")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .build());
        return  sink;
    }


}
