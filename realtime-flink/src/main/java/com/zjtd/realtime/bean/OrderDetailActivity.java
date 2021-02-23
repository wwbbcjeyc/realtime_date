package com.zjtd.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 11:12
 * @Version 1.0
 */
@Data
public class OrderDetailActivity  {



    /**
     * 编号
     */

    private Long id;


    private Long order_detail_id ;

    private Long activity_rule_id;

    private Long sku_id;

    /**
     * 活动id
     */
    private Long activity_id;

    /**
     * 订单编号
     */
    private Long order_id;


    /**
     * 发生日期
     */
    private Date create_time;


    private  String activity_name;

    private  String activity_type;

    private BigDecimal condition_amount;

    private Long condition_num;

    private BigDecimal benefit_amount;

    private BigDecimal  benefit_discount;




}
