package com.zjtd.realtime.publisher.service;

import com.zjtd.realtime.publisher.bean.KeywordStats;

import java.util.List;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 14:00
 * @Version 1.0
 */
public interface KeywordStatsService {

    public List<KeywordStats> getKeywordStats(int date, int limit);
}
