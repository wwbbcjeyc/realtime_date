package com.zjtd.realtime.publisher.controller;

import com.zjtd.realtime.publisher.bean.KeywordStats;
import com.zjtd.realtime.publisher.bean.MidStats;
import com.zjtd.realtime.publisher.bean.ProductStats;
import com.zjtd.realtime.publisher.bean.ProvinceStats;
import com.zjtd.realtime.publisher.service.KeywordStatsService;
import com.zjtd.realtime.publisher.service.MidStatsService;
import com.zjtd.realtime.publisher.service.ProductStatsService;
import com.zjtd.realtime.publisher.service.ProvinceStatsService;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.*;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 13:54
 * @Version 1.0
 */

@RestController
@RequestMapping("/api/sugar")
public class SugarController {

    @Autowired
    MidStatsService midStatsService;

    @Autowired
    ProvinceStatsService provinceStatsService;

    @Autowired
    ProductStatsService productStatsService;

    @Autowired
    KeywordStatsService keywordStatsService; ;

    //    @GetMapping("findAll")
//    public List<MidStats> findAll(){
////        QueryWrapper<MidStats> midStatsQueryWrapper = new QueryWrapper<>();
////        midStatsQueryWrapper.eq("is_new","0");
// //       List<MidStats> list = midStatsService.list(null);
//
//
//   //     return list;
//    }
//    @GetMapping("count")
//    public int count(){
////        QueryWrapper<MidStats> midStatsQueryWrapper = new QueryWrapper<>();
////        midStatsQueryWrapper.eq("is_new","0");
//
//   //     return midStatsService.count(null);
//    }
   /*
{  "status": 0,
    "data": {
        "combineNum": 1,
        "columns": [
            {"name": "类别",
                "id": "type"
            },
            { "name": "新用户",
                "id": "new"
            },
            {"name": "老用户",
                "id": "old"
            }
        ],
        "rows": [
            {
                "type": "用户数",
                "new": 123,
                "old": 13
            },
            {
                "type": "总访问页面",
                "new": 123,
                "old": 145
            },
            {
                "type": "跳出率",
                "new": 123,
                "old": 145
            },
            {
                "type": "平均在线时长",
                "new": 123,
                "old": 145
            },
            {
                "type": "平均访问页面数",
                "new": 23,
                "old": 145
            }
        ]
    }
}
    */

    @RequestMapping("/test")
    public String getTest(){
        return "test";
    }

    @RequestMapping("/midstats")
    public String getMidStatsByNewFlag(@RequestParam(value = "date",defaultValue = "0") Integer date) {
        if(date== 0)  date=now();
        List<MidStats> midStatsByNewFlag = midStatsService.getMidStatsByNewFlag(date);
        MidStats newMidStats=new MidStats();
        MidStats oldMidStats=new MidStats();
        for (MidStats midStats : midStatsByNewFlag) {
            if(midStats.getIs_new().equals("1")){
                newMidStats = midStats;
            }else{
                oldMidStats = midStats;
            }
        }


        String json = "{\"status\":0,\"data\":{\"combineNum\":1,\"columns\":[{\"name\":\"类别\",\"id\":\"type\"},{\"name\":\"新用户\",\"id\":\"new\"},{\"name\":\"老用户\",\"id\":\"old\"}],\"rows\":" +
                "[{\"type\":\"用户数(人)\",\"new\": "+newMidStats.getUv_ct()+ ",\"old\":"+oldMidStats.getUv_ct()+"}," +
                "{\"type\":\"总访问页面(次)\",\"new\":"+newMidStats.getPv_ct()+ ",\"old\":"+oldMidStats.getPv_ct()+"}," +
                "{\"type\":\"跳出率(%)\",\"new\":"+newMidStats.getUjRate()+ ",\"old\":"+oldMidStats.getUjRate()+"}," +
                "{\"type\":\"平均在线时长(秒)\",\"new\":"+newMidStats.getDurPerUv()+ ",\"old\":"+oldMidStats.getDurPerUv()+"}," +
                "{\"type\":\"平均访问页面数(人次)\",\"new\":"+newMidStats.getPvPerUv()+ ",\"old\":"+oldMidStats.getPvPerUv()+"}]}}" ;

        return  json;

    }



    /*
    {
    "status": 0,
    "data": {
        "categories": [
            "01", "02", "03", "04","05"
        ],
        "series": [
            {
                "name": "uv",
                "data": [
                    888065,892945,678379,733572,525091
                ]
            },
            {
                "name": "pv",
                "data": [
                    563998,571831,622419,675294,708512
                ]
            },
            {
                "name": "新用户",
                "data": [
                    563998,571831,622419, 675294,708512
                ]
            }
        ]
    }
}

     */
    @RequestMapping("/hr")
    public String getMidStatsGroupbyHourNewFlag(@RequestParam(value = "date",defaultValue = "0") Integer date ) {
        if(date==0)  date=now();
        List<MidStats> midStatsHrList = midStatsService.getMidStatsGroupbyHourNewFlag(date);
        Map<Integer,MidStats> midStatsHrMap=new HashMap();

        for (MidStats midStats : midStatsHrList) {
            midStatsHrMap.put(midStats.getHr(), midStats);
        }
        List<String> hrList=new ArrayList<>();
        List<Long> uvList=new ArrayList<>();
        List<Long> pvList=new ArrayList<>();
        List<Long> newMidList=new ArrayList<>();
        Iterator<MidStats> iterator = midStatsHrList.iterator();
        for (int i = 0; i <=23 ; i++) {
            MidStats midStats = midStatsHrMap.get(i);
            if (midStats!=null){
                uvList.add(midStats.getUv_ct())   ;
                pvList.add( midStats.getPv_ct());
                newMidList.add( midStats.getNew_uv());
            }else{
                uvList.add(0L)   ;
                pvList.add( 0L);
                newMidList.add( 0L);
            }
            hrList.add(String.format("%02d", i));
        }


        String json = "{\"status\":0,\"data\":{" + "\"categories\":" +
                "[\""+ StringUtils.join(hrList,"\",\"")+ "\"],\"series\":[" +
                "{\"name\":\"uv\",\"data\":["+ StringUtils.join(uvList,",") +"]}," +
                "{\"name\":\"pv\",\"data\":["+ StringUtils.join(pvList,",") +"]}," +
                "{\"name\":\"新用户\",\"data\":["+ StringUtils.join(newMidList,",") +"]}]}}";
        return  json;


    }

    /*
    {
        "status": 0,  "data": 1201067
    }
     */
    public String getPv(Integer date) {

        Long pv = midStatsService.getPv(date);

        String json = "{   \"status\": 0,  \"data\":" + pv + "\"}";
        return  json;
    }


    public Long getUv(Integer date) {
        return midStatsService.getUv(date);
    }


/*
{
    "status": 0,
    "data": {
        "columns": [
            { "name": "商品名称",  "id": "spu_name"
            },
            { "name": "交易额", "id": "order_amount"
            }
        ],
        "rows": [
            {
                "spu_name": "小米10",
                "order_amount": "863399.00"
            },
           {
                "spu_name": "iPhone11",
                "order_amount": "548399.00"
            }
        ]
    }
}
 */

    @RequestMapping("/spu")
    public String getProductStatsGroupBySpu(@RequestParam(value = "date",defaultValue = "0") Integer date, @RequestParam(value = "limit",defaultValue = "10") int limit) {
        if(date==0)  date=now();
        List<ProductStats> statsList = productStatsService.getProductStatsGroupBySpu(date, limit);
        StringBuilder jsonBuilder=new StringBuilder(" {\"status\":0,\"data\":{\"columns\":[{\"name\":\"商品名称\",\"id\":\"spu_name\"},{\"name\":\"交易额\",\"id\":\"order_amount\"},{\"name\":\"订单数\",\"id\":\"order_ct\"}]," +
                "\"rows\":["  );


        for (int i = 0; i < statsList.size(); i++) {
            ProductStats productStats = statsList.get(i);
            if(i>=1){
                jsonBuilder.append(",");
            }
            jsonBuilder.append("{\"spu_name\":\""+productStats.getSpu_name()+"\",\"order_amount\":"+productStats.getOrder_amount()+",\"order_ct\":"+productStats.getOrder_ct()+"}");


        }
        jsonBuilder.append("]}}");
        return  jsonBuilder.toString();
    }

    /*
    {
        "status": 0,
        "data": [
            {
                "name": "数码类",
                "value": 371570
            },
            {
                "name": "日用品",
                "value": 296016
            }
        ]
    }
     */
    @RequestMapping("/category3")
    public String getProductStatsGroupByCategory3(@RequestParam(value = "date",defaultValue = "0") Integer date, @RequestParam(value = "limit",defaultValue = "4") int limit) {
        if(date==0){
            date=now();
        }
        List<ProductStats> statsList = productStatsService.getProductStatsGroupByCategory3(date, limit);

        StringBuilder dataJson=new StringBuilder("{  \"status\": 0, \"data\": [");
        int i=0;
        for (ProductStats productStats : statsList) {
            if(i++ >0){
                dataJson.append(",");
            };
            dataJson.append("{\"name\":\"").append(productStats.getCategory3_name()).append("\",");
            dataJson.append("\"value\":").append(productStats.getOrder_amount()).append("}");
        }
        dataJson.append("]}");
        return dataJson.toString();
    }


    /*
    {
     "status": 0,
     "data": {
         "categories": [
             "三星","vivo","oppo"
         ],
         "series": [
             {
                 "data": [ 406333, 709174, 681971
                 ]
             }
         ]
     }
    }
*/
    @RequestMapping("/trademark")
    public String getProductStatsByTrademark(@RequestParam(value = "date",defaultValue = "0") Integer date, @RequestParam(value = "limit",defaultValue = "20") int limit) {
        if(date==0){
            date=now();
        }
        List<ProductStats> productStatsByTrademarkList = productStatsService.getProductStatsByTrademark(date, limit);
        List<String> tradeMarkList=new ArrayList<>();
        List<BigDecimal> amountList=new ArrayList<>();
        for (ProductStats  productStats : productStatsByTrademarkList) {
            tradeMarkList.add(productStats.getTm_name());
            amountList.add(productStats.getOrder_amount());
        }
        String json = "{\"status\":0,\"data\":{" + "\"categories\":" +
                "[\"" + StringUtils.join(tradeMarkList, "\",\"") + "\"],\"series\":[" +
                "{\"data\":[" + StringUtils.join(amountList, ",") + "]}]}}";
        return  json;

    }

    /*
    {
        "status": 0,  "data": 1201067.1971254142
    }
     */
    private int now(){
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return   Integer.valueOf(yyyyMMdd);
    }

    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date",defaultValue = "0") Integer date) {
        if(date==0){
            date=now();
        }
        BigDecimal gmv = productStatsService.getGMV(date);
        String json = "{   \"status\": 0,  \"data\":" + gmv + "}";
        return  json;
    }

    /*
    {  "status": 0,
        "data": [
            {
                "name": "data",
                "value": 60679,
                "url": "www.baidu.com"
            },
            {
                "name": "dataZoom",
                "value": 24347,
                "url": "www.baidu.com"
            }
        ]
    }
     */
    @RequestMapping("/keyword")
    public String getKeywordStats(@RequestParam(value = "date",defaultValue = "0") Integer date, @RequestParam(value = "limit",defaultValue = "20") int limit){
        if(date==0){
            date=now();
        }
        List<KeywordStats> keywordStatsList = keywordStatsService.getKeywordStats(date, 20);
        StringBuilder jsonSb=new StringBuilder( "{\"status\":0,\"msg\":\"\",\"data\":[" );
        for (int i = 0; i < keywordStatsList.size(); i++) {
            KeywordStats keywordStats =  keywordStatsList.get(i);
            if(i>=1){
                jsonSb.append(",");
            }
            jsonSb.append(  "{\"name\":\"" + keywordStats.getKeyword() + "\",\"value\":"+keywordStats.getCt()+"}");
        }
        jsonSb.append(  "]}");
        return  jsonSb.toString();

    }




    /*
    {
        "status": 0,
        "data": {
            "mapData": [
                {
                    "name": "北京",
                    "value": 9131
                },
                {
                    "name": "天津",
                    "value": 5740
               }
           ]
        }
    }
     */
    @RequestMapping("/province")
    public  String getProvinceStats(@RequestParam(value = "date",defaultValue = "0") Integer date  ) {
        if(date==0){
            date=now();
        }

        StringBuilder jsonBuilder=new StringBuilder(  "{\"status\":0,\"data\":{\"mapData\":[" );
        List<ProvinceStats> provinceStatsList = provinceStatsService.getProvinceStats(date);
        if(provinceStatsList.size()==0){
            //    jsonBuilder.append(  "{\"name\":\"北京\",\"value\":0.00}");
        }
        for (int i = 0; i < provinceStatsList.size(); i++) {
            if(i>=1){
                jsonBuilder.append(",");
            }
            ProvinceStats provinceStats = provinceStatsList.get(i);
            jsonBuilder.append(  "{\"name\":\"" + provinceStats.getProvince_name() + "\",\"value\":"+provinceStats.getOrder_amount()+" }");

        }
        jsonBuilder.append(  "]}}");
        return  jsonBuilder.toString();
    }

}
