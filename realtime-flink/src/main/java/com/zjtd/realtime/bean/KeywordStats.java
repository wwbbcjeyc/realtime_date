package com.zjtd.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 11:28
 * @Version 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordStats {

    private String keyword;
    private Long ct;
    private String source;
    private String stt;
    private String edt;
    private Long ts;

}
