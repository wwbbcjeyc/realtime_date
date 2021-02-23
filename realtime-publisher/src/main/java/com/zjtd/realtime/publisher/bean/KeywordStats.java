package com.zjtd.realtime.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 13:56
 * @Version 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordStats {

    private String stt;
    private String edt;
    private String keyword;
    private Long ct;
    private String ts;

}
