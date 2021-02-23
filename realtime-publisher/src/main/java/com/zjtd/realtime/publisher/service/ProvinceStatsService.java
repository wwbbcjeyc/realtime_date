package com.zjtd.realtime.publisher.service;

import com.zjtd.realtime.publisher.bean.ProvinceStats;

import java.util.List;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 14:00
 * @Version 1.0
 */
public interface ProvinceStatsService {


    public List<ProvinceStats> getProvinceStats(int date);
}
