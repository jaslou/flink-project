package com.jaslou.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author mengqi 2020/6/30 10:36 上午
 */
public class KafkaJoinHbase {
    public static final String  KAFKA_TABLE_SOURCE_DDL = "" +
            "CREATE TABLE user_behavior (\n" +
            "    order_key STRING ," +
            "    order_number STRING ," +
            "    company_code STRING , " +
            "    ts BIGINT," +
            "    proctime AS PROCTIME()  " +
            ") WITH (\n" +
            "    'connector.type' = 'kafka',  -- 指定连接类型是kafka\n" +
            "    'connector.version' = '0.11',  -- 与我们之前Docker安装的kafka版本要一致\n" +
            "    'connector.topic' = 'dtstack_topic', -- 之前创建的topic \n" +
            "    'connector.properties.group.id' = 'flink-test-0', -- 消费者组，相关概念可自行百度\n" +
            "    'connector.startup-mode' = 'latest-offset',  --指定从最早消费\n" +
            "    'connector.properties.zookeeper.connect' = 'localhost:2181',  -- zk地址\n" +
            "    'connector.properties.bootstrap.servers' = 'localhost:9092',  -- broker地址\n" +
            "    'format.type' = 'json'  -- json格式，和topic中的消息格式保持一致\n" +
            ")";

    public static final String HBASE_TABLE_SOURCE_DDL = "" +
            "CREATE TABLE dim_hbase ( " +
            "rowkey String, " +
            "cf ROW<aaa String> " +
            ") WITH ( " +
            "  'connector.type' = 'hbase'," +
            "  'connector.version' = '1.4.3', " +
            "  'connector.table-name' = 'my_hbase'," +
            "  'connector.zookeeper.quorum' = 'localhost:2181'," +
            "  'connector.zookeeper.znode.parent' = '/hbase' " +
            ")";

    public static void main(String[] args) throws Exception {
        //构建StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //构建EnvironmentSettings 并指定Blink Planner
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        //构建StreamTableEnvironment
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

        //通过DDL，注册kafka数据源表
        tEnv.executeSql(KAFKA_TABLE_SOURCE_DDL);
        tEnv.executeSql(HBASE_TABLE_SOURCE_DDL);

        //执行查询
        Table table = tEnv.sqlQuery("select * from user_behavior a inner join" +
                " dim_hbase b on a.order_key = b.rowkey");


        String sql = "insert into dim_hbase select order_key, row(order_number) as cf from user_behavior";

        tEnv.executeSql(sql);

        //转回DataStream并输出
        tEnv.toRetractStream(table, Row.class).print().setParallelism(1);

        //任务启动，这行必不可少！
        env.execute("test");

    }
}

