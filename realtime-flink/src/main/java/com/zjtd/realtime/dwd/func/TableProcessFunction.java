package com.zjtd.realtime.dwd.func;

import com.alibaba.fastjson.JSONObject;

import com.zjtd.realtime.bean.TableProcess;
import com.zjtd.realtime.common.GmallConfig;
import com.zjtd.realtime.util.MySQLUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 10:42
 * @Version 1.0
 */
public class TableProcessFunction extends ProcessFunction<JSONObject, JSONObject> {

    OutputTag<JSONObject> hbaseTag  =null ;

    public TableProcessFunction(OutputTag<JSONObject> hbaseTag ){
        this.hbaseTag =hbaseTag;
    }

    Map<String, TableProcess> tableProcessMap = null;

    Set<String> existsTables=new HashSet<>();

    public Map refreshMeta() {
        System.out.println("更新处理信息！");
        Map<String, TableProcess> tableProcessMap = new HashMap();
        List<TableProcess> tableProcessList = MySQLUtil.queryList("select * from  table_process", TableProcess.class, true);
        for (TableProcess tableProcess : tableProcessList) {
            String key = tableProcess.getSourceTable() + ":" + tableProcess.getOperateType();
            tableProcessMap.put(key, tableProcess);
            if("insert".equals( tableProcess.getOperateType())&&"hbase".equals(tableProcess.getSinkType())){
                boolean noExists = existsTables.add(tableProcess.getSourceTable());
                if(noExists) {
                    checkTable(tableProcess.getSinkTable(), tableProcess.getSinkColumns(), tableProcess.getSinkPk(), tableProcess.getSinkExtend());
                }
            }
        }
        if(tableProcessMap.size()==0){
            throw new RuntimeException("缺少处理信息");
        }
        return tableProcessMap;
    }


    private JSONObject filterColumn(JSONObject data ,String  sinkColumns){
        String[] cols = StringUtils.split(sinkColumns, ",");
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        List<String> columnList = Arrays.asList(cols);
        for (Iterator<Map.Entry<String, Object>> iterator = entries.iterator(); iterator.hasNext(); ) {
            Map.Entry<String, Object> entry = iterator.next();
            if(!columnList.contains(entry.getKey()) ){
                iterator.remove();
            }
        }
        return data;
    }

    @Override
    public void processElement(JSONObject jsonObject, Context ctx, Collector<JSONObject> out) throws Exception {
        String sourceTable = jsonObject.getString("table");
        String optType = jsonObject.getString("type");
        JSONObject dataJsonObj = jsonObject.getJSONObject("data");
        if(dataJsonObj==null||dataJsonObj.size()<3){
            return;
        }

        if(optType.equals("bootstrap-insert")){
            optType="insert";
            jsonObject.put("type","insert");
        }
        if (tableProcessMap != null&&tableProcessMap.size()>0) {
            String key = sourceTable + ":" + optType;
            TableProcess tableProcess = tableProcessMap.get(key);
            if(tableProcess!=null){
                jsonObject.put("sink_table",tableProcess.getSinkTable());
                if(tableProcess.getSinkColumns()!=null &&tableProcess.getSinkColumns().length()>0) {
                    filterColumn(jsonObject.getJSONObject("data"), tableProcess.getSinkColumns());
                }
            }else{
                System.out.println("no this key :"+key);
            }

            if (tableProcess!=null&&TableProcess.SINK_TYPE_HBASE.equalsIgnoreCase(tableProcess.getSinkType()) ) {
                ctx.output(hbaseTag, jsonObject);
            } else if(tableProcess!=null&&TableProcess.SINK_TYPE_KAFKA.equalsIgnoreCase(tableProcess.getSinkType())) {
                out.collect(jsonObject);
            }
        }

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        connection   = DriverManager.getConnection("jdbc:phoenix:hdp1,hdp2,hdp3:2181" );
        tableProcessMap= refreshMeta();
        Timer timer=new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                tableProcessMap= refreshMeta();
            }
        },5000,5000);

    }


    public Connection connection=null;



    public void checkTable(String tableName, String  fields,String pk ,String ext){
        if(pk==null){
            pk="id";
        }
        if(ext==null){
            ext = "";
        }

        StringBuilder createSql=new StringBuilder("create table if not exists  "+ GmallConfig.HBASE_SCHEMA +"."+tableName+"(");
        String[] fieldsArr = fields.split(",");
        for (int i = 0; i < fieldsArr.length; i++) {
            String field = fieldsArr[i];

            if(field.equals(pk)){
                createSql.append( field+" varchar");
                createSql.append(" primary key ");
            }else{
                createSql.append("info."+field+" varchar");
            }
            if(i<fieldsArr.length-1){
                createSql.append(",");
            }

        }

        createSql.append(")");
        createSql.append( ext);
        try{
            Statement stat   = connection.createStatement();
            System.out.println(createSql);
            stat.execute(  createSql.toString());
            stat.close();

        } catch ( Exception e) {
            e.printStackTrace();
            throw  new RuntimeException("建表失败！");
        }

    }

}
