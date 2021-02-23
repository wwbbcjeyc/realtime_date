package com.zjtd.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 11:06
 * @Version 1.0
 */
@Data
public class PaymentInfo {

    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String payment_time;
}
