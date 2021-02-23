package com.zjtd.realtime.bean;

import lombok.Data;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 11:09
 * @Version 1.0
 */
@Data
public class PaymentWide {

    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String payment_time;

    Long detail_id;
    Long sku_id;
    BigDecimal order_price ;
    Long sku_num ;
    String sku_name;


    Long province_id;
    String order_status;
    BigDecimal final_total_amount;
    BigDecimal benefit_reduce_amount;
    BigDecimal original_total_amount;
    BigDecimal final_detail_amount ;
    BigDecimal feight_fee;
    String expire_time;
    String create_time;
    String operate_time;
    String create_date; // 把其他字段处理得到
    String create_hour;


    public PaymentWide(PaymentInfo paymentInfo,OrderWide orderWide){
        mergeOrderWide(orderWide);
        mergePaymentInfo(paymentInfo);


    }

    public void  mergeOrderWide(OrderWide orderWide  )  {
        if (orderWide != null) {
            try {
                BeanUtils.copyProperties(this,orderWide);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }

        }
    }

    public void  mergePaymentInfo(PaymentInfo paymentInfo  )  {
        if (paymentInfo != null) {
            try {
                BeanUtils.copyProperties(this,paymentInfo);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }


        }
    }
}
