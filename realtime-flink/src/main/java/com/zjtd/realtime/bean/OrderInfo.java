package com.zjtd.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 11:14
 * @Version 1.0
 */
@Data
public class OrderInfo {

    public static final String ORDER_STATUS_UNPAID="1001";
    public static final String ORDER_STATUS_PAID="1002";

    Long id;
    Long province_id;
    String order_status;
    Long user_id;
    BigDecimal total_amount;
    BigDecimal activity_reduce_amount;
    BigDecimal coupon_reduce_amount;
    BigDecimal original_total_amount;
    BigDecimal feight_fee;
    String expire_time;
    String create_time;
    String operate_time;
    String create_date; // 把其他字段处理得到
    String create_hour;

    Long create_ts;


}
