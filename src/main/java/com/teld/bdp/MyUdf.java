package com.teld.bdp;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

public class MyUdf extends ScalarFunction {
    /**
     * 1. 将表示基本信息的字符串转换为Row对象，此处的Row对象并没有ColumnName标识
     * @param originColumn
     * @return
     */
    public Row eval(String originColumn) {
        // originColumn = "1:Zhangsan:1985"
        String[] tempArray = originColumn.split(":");
        Row row = new Row(3);
        row.setField(0, Integer.valueOf(tempArray[0]));
        row.setField(1, tempArray[1]);
        row.setField(2, Integer.valueOf(tempArray[2]));
        return row;
    }

    /**
     * 2. 对函数返回的Row对象的TypeInformation加入ColumnName的标识
     * @param signature
     * @return
     */
    @Override
    public TypeInformation<Row> getResultType(Class<?>[] signature) {
        String[] names = new String[]{"Id", "Name", "Birth"};
        TypeInformation<?>[] types = new TypeInformation[]{Types.INT(), Types.STRING(), Types.INT()};
        return Types.ROW(names, types);
    }
}
