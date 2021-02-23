package com.zjtd.realtime.dws.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.google.common.cache.CacheBuilder;
import com.zjtd.realtime.bean.ProductStats;
import com.zjtd.realtime.util.DimUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 14:58
 * @Version 1.0
 */
@Slf4j
public class SkuDimAsyncFunction extends RichAsyncFunction<ProductStats, ProductStats> {

    ExecutorService executorService = null;

    public void  open(Configuration parameters  ) {

        System.out.println ("开辟线程池！！！！！");
        executorService = Executors.newFixedThreadPool(3);


    }
    @Override
    public void asyncInvoke(ProductStats productStats, ResultFuture<ProductStats> resultFuture) throws Exception {
        executorService.submit(new Runnable() {

            @Override
            public void run() {
                Long skuId = productStats.getSku_id();
                JSONObject skuJsonObj = DimUtil.getDimInfo("dim_sku_info", String.valueOf(skuId));
                if(skuJsonObj !=null){
                    productStats.setSku_name(skuJsonObj.getString("SKU_NAME"));
                    productStats.setSku_price(skuJsonObj.getBigDecimal("PRICE"));

                    String category3Id = skuJsonObj.getString("CATEGORY3_ID");
                    if(category3Id!=null){
                        productStats.setCategory3_id(Long.valueOf(category3Id));
                        JSONObject category3JsonObj = DimUtil.getDimInfo("dim_base_category3", category3Id);
                        if(category3JsonObj!=null){
                            productStats.setCategory3_id(category3JsonObj.getLong("ID"));
                            productStats.setCategory3_name(category3JsonObj.getString("NAME"));
                        }
                    }
                    String spuId = skuJsonObj.getString("SPU_ID");
                    if(spuId!=null){
                        productStats.setSpu_id(Long.valueOf(spuId));
                        JSONObject spuJsonObj = DimUtil.getDimInfo("dim_spu_info", spuId);
                        if(spuJsonObj!=null) {
                            productStats.setSpu_name(spuJsonObj.getString("SPU_NAME"));
                        }

                    }
                    String tmId = skuJsonObj.getString("TM_ID");
                    if(tmId.equals("1")){
                        System.out.println("11");
                    }
                    if(tmId!=null){
                        productStats.setTm_id(Long.valueOf(tmId));
                        JSONObject tmJsonObj = DimUtil.getDimInfo("dim_base_trademark", tmId);
                        if(tmJsonObj!=null){
                            productStats.setTm_name(tmJsonObj.getString("TM_NAME"));
                        }
                    }

                }

                resultFuture.complete(Arrays.asList(productStats));
            }

        });

    }






}
