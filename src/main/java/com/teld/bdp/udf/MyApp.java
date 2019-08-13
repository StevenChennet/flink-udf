package com.teld.bdp.udf;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class MyApp {
    public static void main(String[] args) throws Exception{
        Configuration localConfig = new Configuration();
        localConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI( localConfig );

        DataStreamSource<String> source = env.fromElements("1:Zhangsan:1985","2:Lisi:1990","3:Wangwu:1995");
        StreamTableEnvironment ste = TableEnvironment.getTableEnvironment(env);
        ste.registerFunction("CONVERTORIGIN", new MyUdf());

        ste.registerDataStream("sourceTable", source, "originColumn");
        System.out.println("原始sourceTable的结构信息");
        ste.scan("sourceTable").printSchema();

        Table tempTable = ste.sqlQuery("SELECT CONVERTORIGIN(originColumn) FROM sourceTable");
        ste.registerTable("TempTable", tempTable);

        Table targetTable = ste.sqlQuery("SELECT Id,Name,Birth FROM TempTable");
        System.out.println("");
        System.out.println("拆分成三列的targetTable的结构信息");
        targetTable.printSchema();

        ste.toAppendStream(targetTable, Row.class).print();

        env.execute("App");
    }
}
