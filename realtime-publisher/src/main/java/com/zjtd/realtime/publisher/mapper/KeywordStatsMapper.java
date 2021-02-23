package com.zjtd.realtime.publisher.mapper;

import com.zjtd.realtime.publisher.bean.KeywordStats;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 14:07
 * @Version 1.0
 */
@Mapper
public interface KeywordStatsMapper {

    @Select("select keyword,  sum(keyword_stats.ct * multiIf(source='SEARCH',5,source='ORDER',2,source='CART',1,0)) ct  from keyword_stats where toYYYYMMDD(stt)=#{date}  group by keyword order by sum(keyword_stats.ct) limit #{limit} ")
    public List<KeywordStats> selectKeywordStats(@Param("date") int date,@Param("limit") int limit);
}
