package com.zjtd.realtime.publisher.mapper;

import com.zjtd.realtime.publisher.bean.ProvinceStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 14:10
 * @Version 1.0
 */
public interface ProvinceStatsMapper {

    @Select("select province_name , sum(order_amount) order_amount  from province_stats where toYYYYMMDD(stt)=#{date} group by province_id ,province_name  ")
    public List<ProvinceStats> selectProvinceStats(int date);
}
