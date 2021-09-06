package com.jaslou.sql;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


import java.util.Arrays;

public class SQLDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settsettingsings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,settsettingsings);


        DataStream<Order> sourceA = env.fromCollection(Arrays.asList(
                new Order(1L, "beer", 5),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)
        ));

        /*DataStream<Order> sourceB = env.fromElements(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)
        );*/

        DataStream<Order> sourceB = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)));

        Table tableA = tableEnv.fromDataStream(sourceA, "user, product, amount");
        tableEnv.registerDataStream("tableB",  sourceB, "user, product, amount");

        /*Table result = tableEnv.sqlQuery("SELECT * FROM " + tableA + " WHERE amount > 2 UNION ALL " +
                "SELECT * FROM tableB WHERE amount < 2");*/

        Table result = tableEnv.sqlQuery("SELECT * FROM " + tableA );

        result.printSchema();

        System.out.println(result.getSchema());

        System.out.println(result.getQueryOperation().getTableSchema());

        DataStream<Order> orderDataStream = tableEnv.toAppendStream(result, Order.class);

        orderDataStream.print();

        env.execute(" sql demo");

    }

}


