package com.zjtd.realtime.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.serializer.SerializeConfig;
import lombok.Data;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;

/**
 * @Author Wang wenbo
 * @Date 2021/2/20 16:47
 * @Version 1.0
 */
public class JsonUtil {

    public static void main(String[] args) {
        String json="{\"name\":\"zhang3\",\"data\":{\"balance\":1000,\"amount\":330}}";
        System.out.println(JSON.parseObject(json).getJSONObject("data.$amount"));
        String customerJson="{\"first_name\":\"zhang3\"}";
        SerializeConfig config = new SerializeConfig();
        ParserConfig pconfig = new ParserConfig();
//        pconfig.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;
        Customer c = JSON.parseObject(customerJson, Customer.class   );
        System.out.println(c);

        Customer customer = new Customer();
        customer.lastName="zc";

        try {
            BeanUtils.copyProperties(c,customer);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        System.out.println(c);


    }

}
