package com.zjtd.realtime.dws.func;

import com.zjtd.realtime.common.GmallConstant;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @Author Wang wenbo
 * @Date 2021/2/23 11:30
 * @Version 1.0
 */
@FunctionHint(output = @DataTypeHint("ROW<ct BIGINT,source STRING>"))
public class KeywordProductC2RUDTF extends TableFunction<Row> {
    public void eval(Long clickCt, Long cartCt, Long orderSkuNum) {

        if(clickCt>0L) {
            Row rowClick = new Row(2);
            rowClick.setField(0, clickCt);
            rowClick.setField(1, GmallConstant.KEYWORD_CLICK);
            collect(rowClick);
        }
        if(cartCt>0L) {
            Row rowCart = new Row(2);
            rowCart.setField(0, cartCt);
            rowCart.setField(1, GmallConstant.KEYWORD_CART);
            collect(rowCart);
        }
        if(orderSkuNum>0) {
            Row rowOrder = new Row(2);
            rowOrder.setField(0, orderSkuNum);
            rowOrder.setField(1, GmallConstant.KEYWORD_ORDER);
            collect(rowOrder);
        }

    }
}
