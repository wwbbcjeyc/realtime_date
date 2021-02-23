package com.zjtd.realtime.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 13:52
 * @Version 1.0
 */

@SpringBootApplication
@MapperScan("com.zjtd.realtime.publisher.mapper")
public class RealtimePublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(RealtimePublisherApplication.class, args);
    }

}
