package com.zjtd.realtime.bean;

import lombok.Data;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 11:29
 * @Version 1.0
 */
@Data
public class UserJumpStats {
    private String stt;
    private String vc;
    private String ch;
    private String ar;
    private String is_new;
    private Long uj_ct=0L;

}
