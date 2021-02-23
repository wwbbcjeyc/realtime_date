package com.zjtd.realtime.publisher.service.impl;

import com.zjtd.realtime.publisher.bean.ProvinceStats;
import com.zjtd.realtime.publisher.mapper.ProvinceStatsMapper;
import com.zjtd.realtime.publisher.service.ProvinceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 14:09
 * @Version 1.0
 */
@Service
public class ProvinceStatsServiceImpl implements ProvinceStatsService {

    @Autowired
    ProvinceStatsMapper provinceStatsMapper;
    @Override
    public List<ProvinceStats> getProvinceStats(int date) {
        return provinceStatsMapper.selectProvinceStats(date);
    }
}
