package com.zjtd.realtime.publisher.mapper;

import com.zjtd.realtime.publisher.bean.ProductStats;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 14:10
 * @Version 1.0
 */
@Mapper
public interface ProductStatsMapper {


    @Select("select  spu_id,spu_name ,  sum(product_stats.order_amount) order_amount ,  sum(product_stats.order_ct) order_ct  from product_stats where toYYYYMMDD(stt)=#{date} group by   spu_id,spu_name  having order_amount>0  order by  order_amount  desc limit #{limit} ")
    public List<ProductStats> getProductStatsGroupBySpu(@Param("date") int date, @Param("limit") int limit);

    @Select("select category3_id,category3_name,  sum(product_stats.order_amount) order_amount   from product_stats where toYYYYMMDD(stt)=#{date} group by   category3_id,category3_name having order_amount>0  order by  order_amount desc limit #{limit}")
    public List<ProductStats> getProductStatsGroupByCategory3(@Param("date")int date , @Param("limit") int limit);

    @Select("select tm_id,tm_name,  sum(product_stats.order_amount) order_amount   from product_stats where toYYYYMMDD(stt)=#{date} group by   tm_id,tm_name having order_amount>0  order by  order_amount  desc limit #{limit} ")
    public List<ProductStats> getProductStatsByTrademark(@Param("date")int date,  @Param("limit") int limit);

    @Select("select   sum(product_stats.order_amount) order_amount  from product_stats where toYYYYMMDD(stt)=#{date}")
    public BigDecimal getGMV(int date);
}
