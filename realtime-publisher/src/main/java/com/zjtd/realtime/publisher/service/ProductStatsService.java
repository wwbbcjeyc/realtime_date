package com.zjtd.realtime.publisher.service;

import com.zjtd.realtime.publisher.bean.ProductStats;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 14:00
 * @Version 1.0
 */
public interface ProductStatsService {


    public List<ProductStats> getProductStatsGroupBySpu(int date, int limit);

    public List<ProductStats> getProductStatsGroupByCategory3(int date,int limit);

    public List<ProductStats> getProductStatsByTrademark(int date,int limit);

    public BigDecimal getGMV(int date);

}
