package com.zjtd.realtime.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 13:57
 * @Version 1.0
 */
@AllArgsConstructor
@Data
@NoArgsConstructor
public class ProvinceStats {

    private String stt;
    private String edt;
    private String province_id;
    private String province_name;
    private BigDecimal order_amount;
    private String ts;
}
