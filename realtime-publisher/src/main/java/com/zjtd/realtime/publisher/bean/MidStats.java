package com.zjtd.realtime.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 13:56
 * @Version 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public   class MidStats {


    private String stt;

    private String edt;

    private String vc;

    private String ch;

    private String ar;


    private String is_new;

    private Long uv_ct=0L;

    private Long pv_ct=0L;

    private Long sv_ct=0L;

    private Long uj_ct=0L;

    private Long dur_sum=0L;


    private Long new_uv=0L;


    private Long ts;

    private int hr;


    public BigDecimal getUjRate(){
        if(uv_ct!=0L){
            return     BigDecimal.valueOf(uj_ct).multiply(BigDecimal.valueOf(100)).divide( BigDecimal.valueOf(sv_ct),2, RoundingMode.HALF_UP) ;
        }else{
            return  BigDecimal.ZERO;
        }
    }

    public BigDecimal getDurPerUv(){
        if(uv_ct!=0L) {
            return BigDecimal.valueOf(dur_sum).divide(BigDecimal.valueOf(sv_ct),0,RoundingMode.HALF_UP).divide(BigDecimal.valueOf(1000),1,RoundingMode.HALF_UP);
        }else {
            return BigDecimal.ZERO;
        }
    }



    public BigDecimal getPvPerUv(){
        if(uv_ct!=0L) {
            return BigDecimal.valueOf(pv_ct).divide(BigDecimal.valueOf(sv_ct), 2, RoundingMode.HALF_UP);
        }else{
            return BigDecimal.ZERO;
        }
    }
}
