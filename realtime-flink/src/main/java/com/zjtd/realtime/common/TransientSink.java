package com.zjtd.realtime.common;

/**
 * @Author Wang wenbo
 * @Date 2021/2/20 16:39
 * @Version 1.0
 */
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;


@Target(FIELD)
@Retention(RUNTIME)
public @interface TransientSink {
}