package com.zjtd.realtime.publisher.mapper;

import com.zjtd.realtime.publisher.bean.MidStats;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 14:08
 * @Version 1.0
 */
@Mapper
public interface MidStatsMapper {
// extends BaseMapper<MidStats>

    @Select("select  is_new ,  sum(uv_ct) uv_ct, sum(pv_ct) pv_ct,sum(sv_ct) sv_ct, sum(uj_ct) uj_ct, sum(dur_sum) dur_sum    from mid_stats where toYYYYMMDD(stt)=#{date} group by  is_new")
    public List<MidStats> selectMidStatsByNewFlag(int date);

    @Select("select  sum(if(is_new='1', mid_stats.uv_ct,0)) new_uv, toHour(stt) hr, sum(uv_ct) uv_ct, sum(pv_ct) pv_ct, sum(uj_ct) uj_ct  from mid_stats where toYYYYMMDD(stt)=#{date} group by  is_new ,toHour(stt)")
    public List<MidStats> selectMidStatsGroupbyHourNewFlag(int date);

    @Select("select   count(mid_stats.pv_ct) pv_ct from mid_stats where toYYYYMMDD(stt)=#{date}  ")
    public Long selectPv(int date);

    @Select("select   count(mid_stats.uv_ct) uv_ct from mid_stats where toYYYYMMDD(stt)=#{date}  ")
    public Long selectUv(int date);
}