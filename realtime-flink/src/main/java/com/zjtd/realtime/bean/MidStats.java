package com.zjtd.realtime.bean;

import com.alibaba.fastjson.JSONObject;
import com.zjtd.realtime.common.TransientSink;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.HashSet;
import java.util.Set;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 15:07
 * @Version 1.0
 */
@Data
@AllArgsConstructor
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
    private Long ts;

    @TransientSink
    private Set<JSONObject> midStatsSet=new HashSet<>();


}
