package com.zjtd.realtime.bean;

import lombok.Data;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 10:49
 * @Version 1.0
 */
@Data
public class TableProcess  {

    public static final String SINK_TYPE_HBASE="HBASE";
    public static final String SINK_TYPE_KAFKA="KAFKA";
    public static final String SINK_TYPE_CK="CLICKHOUSE";

    String sourceTable;

    String operateType;

    String sinkType;

    String sinkTable;

    String sinkColumns;

    String  sinkPk;

    String sinkExtend;


}
