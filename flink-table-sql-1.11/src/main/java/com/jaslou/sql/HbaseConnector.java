package com.jaslou.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author mengqi 2020/6/30 10:36 上午
 */
public class HbaseConnector {

    public static final String HBASE_TABLE_SOURCE_DDL = "" +
            "CREATE TABLE my_hbase ( " +
            " rowkey STRING," +
            " cf ROW<user_id String>, " +
            " PRIMARY KEY (rowkey) NOT ENFORCED" +
            ") WITH ( " +
            "  'connector' = 'hbase-1.4',\n" +
            "  'table-name' = 'mytable',\n" +
            "  'zookeeper.quorum' = 'localhost:2181',\n" +
            "  'zookeeper.znode.parent' = '/hbase' " +
            ")";

    public static void main(String[] args) throws Exception {
        //构建StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //构建EnvironmentSettings 并指定Blink Planner
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        //构建StreamTableEnvironment
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

        //通过DDL，注册kafka数据源表
        tEnv.executeSql(HBASE_TABLE_SOURCE_DDL);

        //执行查询
        Table table = tEnv.sqlQuery("select * from my_hbase");

        //转回DataStream并输出
        tEnv.toRetractStream(table, Row.class).print().setParallelism(1);

        //任务启动，这行必不可少！
        env.execute("test");

    }
}

