package com.zjtd.realtime.publisher.service.impl;

import com.zjtd.realtime.publisher.bean.KeywordStats;
import com.zjtd.realtime.publisher.mapper.KeywordStatsMapper;
import com.zjtd.realtime.publisher.service.KeywordStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 14:06
 * @Version 1.0
 */
@Service
public class KeywordStatsServiceImpl implements KeywordStatsService {

    @Autowired
    KeywordStatsMapper keywordStatsMapper;

    @Override
    public List<KeywordStats> getKeywordStats(int date, int limit) {
        return keywordStatsMapper.selectKeywordStats(date,limit);
    }
}
