package com.zjtd.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 11:13
 * @Version 1.0
 */
@Data
public class OrderDetailCoupon {



    /**
     * 编号
     */

    private Long id;


    private Long order_detail_id ;

    private Long coupon_id;

    private Long sku_id;


    /**
     * 订单编号
     */
    private Long order_id;


    /**
     * 发生日期
     */
    private Date create_time;



    private String coupon_name;

    private String coupon_type;

    private BigDecimal condition_amount;

    private Long condition_num;

    private Long activity_id;

    private BigDecimal benefit_amount;

    private BigDecimal  benefit_discount;






}
