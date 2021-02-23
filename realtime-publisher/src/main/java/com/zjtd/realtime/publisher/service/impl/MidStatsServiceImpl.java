package com.zjtd.realtime.publisher.service.impl;

import com.zjtd.realtime.publisher.bean.MidStats;
import com.zjtd.realtime.publisher.mapper.MidStatsMapper;
import com.zjtd.realtime.publisher.service.MidStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 14:07
 * @Version 1.0
 */
@Service
public class MidStatsServiceImpl  implements MidStatsService {
    // extends ServiceImpl<MidStatsMapper, MidStats>
    @Autowired
    MidStatsMapper midStatsMapper;

    @Override
    public List<MidStats> getMidStatsByNewFlag(int date) {
        return midStatsMapper.selectMidStatsByNewFlag(date);
    }

    @Override
    public List<MidStats> getMidStatsGroupbyHourNewFlag(int date) {
        return midStatsMapper.selectMidStatsGroupbyHourNewFlag(date);
    }

    @Override
    public Long getPv(int date) {
        return midStatsMapper.selectPv(date);
    }

    @Override
    public Long getUv(int date) {
        return midStatsMapper.selectUv(date);
    }
}
