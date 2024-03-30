package com.atguigu.gmall.realtime.dws.function;

import com.atguigu.gmall.realtime.common.util.IkUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author yhm
 * @create 2023-12-27 9:36
 */
@FunctionHint(output = @DataTypeHint("ROW<keyword STRING>"))
public class KwSplit extends TableFunction<Row> {

    public void eval(String keywords) {
        List<String> stringList = IkUtil.IKSplit(keywords);
        for (String s : stringList) {
            collect(Row.of(s));
        }

    }
}
