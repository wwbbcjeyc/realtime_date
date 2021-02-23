package com.zjtd.realtime.dws.func;

import com.alibaba.fastjson.JSONObject;
import com.zjtd.realtime.bean.ProvinceStats;
import com.zjtd.realtime.util.DimUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 14:58
 * @Version 1.0
 */
public class ProvinceDimAsyncFunction extends RichAsyncFunction<ProvinceStats, ProvinceStats> {

    ExecutorService executorService = null;

    public void  open(Configuration parameters  ) {

        System.out.println ("开辟线程池！！！！！");
        executorService = Executors.newFixedThreadPool(3);


    }
    @Override
    public void asyncInvoke(ProvinceStats provinceStats, ResultFuture<ProvinceStats> resultFuture) throws Exception {
        executorService.submit(new Runnable() {

            @Override
            public void run() {
                Long provinceId = provinceStats.getProvince_id();
                JSONObject provinceJsonObj = DimUtil.getDimInfo("dim_base_province", String.valueOf(provinceId));
                if(provinceJsonObj!=null){
                    provinceStats.setProvince_name(provinceJsonObj.getString("NAME"));
                    provinceStats.setArea_code(provinceJsonObj.getString("AREA_CODE"));
                    provinceStats.setIso_code(provinceJsonObj.getString("ISO_CODE"));
                    provinceStats.setIso_3166_2(provinceJsonObj.getString("ISO_3166_2"));
                }
                resultFuture.complete(Arrays.asList(provinceStats));
            }

        });

    }






}
