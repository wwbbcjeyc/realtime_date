package com.zjtd.realtime.bean;

import lombok.Data;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 11:29
 * @Version 1.0
 */
@Data
public class PageStats {
    private String stt;
    private String edt;
    private String page_id;
    private String last_page_id;
    private Long pv_ct=0L;
    private Long dur_avg=0L;
    private Long ts;

}
