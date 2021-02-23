package com.zjtd.realtime.dws.func;

import com.zjtd.realtime.util.KeywordUtil;
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
@FunctionHint(output = @DataTypeHint("ROW<s STRING >"))
public class KeywordUDTF  extends TableFunction<Row> {
    public void eval(String value) {
        List<String> keywordList = KeywordUtil.analyze(value);
        for (String keyword : keywordList) {
            Row row = new Row(1);
            row.setField(0,keyword);
            collect(row);
        }
    }
}
