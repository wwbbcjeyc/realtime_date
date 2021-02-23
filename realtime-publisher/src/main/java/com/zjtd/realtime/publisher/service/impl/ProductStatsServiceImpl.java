package com.zjtd.realtime.publisher.service.impl;

import com.zjtd.realtime.publisher.bean.ProductStats;
import com.zjtd.realtime.publisher.mapper.ProductStatsMapper;
import com.zjtd.realtime.publisher.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 14:09
 * @Version 1.0
 */
@Service
public class ProductStatsServiceImpl  implements ProductStatsService {

    @Autowired
    ProductStatsMapper productStatsMapper;

    @Override
    public List<ProductStats> getProductStatsGroupBySpu(int date,int limit) {
        return productStatsMapper.getProductStatsGroupBySpu(date,  limit);
    }

    @Override
    public List<ProductStats> getProductStatsGroupByCategory3(int date, int limit) {
        return productStatsMapper.getProductStatsGroupByCategory3(date,  limit);
    }

    @Override
    public List<ProductStats> getProductStatsByTrademark(int date, int limit) {
        return productStatsMapper.getProductStatsByTrademark(date,  limit);
    }

    @Override
    public BigDecimal getGMV(int date) {
        return productStatsMapper.getGMV(date);
    }
}
