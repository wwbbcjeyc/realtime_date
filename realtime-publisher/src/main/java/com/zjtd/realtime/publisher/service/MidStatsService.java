package com.zjtd.realtime.publisher.service;

import com.zjtd.realtime.publisher.bean.MidStats;

import java.util.List;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 14:00
 * @Version 1.0
 */
public interface MidStatsService {
    // extends IService<MidStats>

    public List<MidStats> getMidStatsByNewFlag(int date);

    public List<MidStats> getMidStatsGroupbyHourNewFlag(int date);

    public Long getPv(int date);

    public Long getUv(int date);
}
