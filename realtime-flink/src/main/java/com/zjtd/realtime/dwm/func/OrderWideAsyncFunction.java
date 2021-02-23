package com.zjtd.realtime.dwm.func;


import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.zjtd.realtime.bean.OrderWide;
import com.zjtd.realtime.bean.PaymentInfo;
import com.zjtd.realtime.bean.PaymentWide;
import com.zjtd.realtime.util.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.mortbay.log.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 11:05
 * @Version 1.0
 */
public class OrderWideAsyncFunction extends RichAsyncFunction<PaymentInfo, PaymentWide> {



    Cache<Long, Set<OrderWide>> orderWideCache = null;
    ExecutorService executorService = null;

    public void  open(Configuration parameters  ) {

        System.out.println ("开辟线程池！！！！！");
        executorService = Executors.newFixedThreadPool(3);

        PhoenixUtil.queryInit();
        orderWideCache = CacheBuilder.newBuilder()
                .concurrencyLevel(12)
                .expireAfterAccess(5, TimeUnit.HOURS)
                .maximumSize(100).build();
    }

    @Override
    public void asyncInvoke(PaymentInfo paymentInfo, ResultFuture<PaymentWide> resultFuture) throws Exception {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                Set<OrderWide> orderWideSet = orderWideCache.getIfPresent(paymentInfo.getOrder_id());
                if (orderWideSet == null || orderWideSet.size() == 0) {
                    Log.debug("开始查询");
                    String sql = "select * from gmall_order_wide where order_id='" + paymentInfo.getOrder_id() + "'";
                    Log.debug(sql);
                    List<OrderWide> orderWideList = PhoenixUtil.queryList(sql, OrderWide.class);
                    List<PaymentWide> paymentWideList = new ArrayList<>();
                    for (OrderWide orderWide : orderWideList) {
                        paymentWideList.add(new PaymentWide(paymentInfo, orderWide));
                    }
                    resultFuture.complete(paymentWideList);
                } else {
                    Log.debug("命中缓存");
                    List<PaymentWide> paymentWideList = new ArrayList<>();
                    for (OrderWide orderWide : orderWideSet) {
                        paymentWideList.add(new PaymentWide(paymentInfo, orderWide));
                    }
                    resultFuture.complete(paymentWideList);
                }
            }
        });

    }



}
