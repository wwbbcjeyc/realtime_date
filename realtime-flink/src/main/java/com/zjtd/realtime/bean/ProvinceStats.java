package com.zjtd.realtime.bean;

import com.zjtd.realtime.common.TransientSink;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 15:02
 * @Version 1.0
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class ProvinceStats {

    private String stt;
    private String edt;
    private Long province_id;
    private String province_name;
    private String area_code;
    private String iso_code;
    private String iso_3166_2;
    private BigDecimal order_amount;
    private Long  order_count;

    @TransientSink
    private Set orderIdSet=new HashSet();
    private Long ts;


    public ProvinceStats(OrderWide orderWide){
        province_id = orderWide.getProvince_id();
        order_amount = orderWide.getSplit_total_amount();
        orderIdSet.add(orderWide.getOrder_id());
        order_count = 1L;
        ts=new Date().getTime();
    }
}
