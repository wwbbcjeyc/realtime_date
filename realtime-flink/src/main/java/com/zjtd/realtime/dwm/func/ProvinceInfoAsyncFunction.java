package com.zjtd.realtime.dwm.func;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.zjtd.realtime.bean.OrderInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;


import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 11:19
 * @Version 1.0
 */
public class ProvinceInfoAsyncFunction extends RichAsyncFunction<OrderInfo, OrderInfo> {

    Cache<String, Map<String, String>> provinceInfoCache =null;
    ExecutorService executorService=null;

    @Override
    public void open(Configuration conf)  throws  Exception  {
        super.open(conf);
        System.out.println("开辟线程池！！！！！");
        executorService = Executors.newFixedThreadPool(3);

        provinceInfoCache = CacheBuilder.newBuilder()
                .concurrencyLevel(12)
                .expireAfterWrite(5, TimeUnit.HOURS)
                .maximumSize(100).build();
    }


    @Override
    public void asyncInvoke(  OrderInfo input,   ResultFuture<OrderInfo> resultFuture) {



    }
}
