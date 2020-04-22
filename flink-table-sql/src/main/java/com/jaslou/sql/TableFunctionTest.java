package com.jaslou.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Arrays;

public class TableFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        env.setParallelism(1);

        DataStream<Order> sourceA = env.fromCollection(Arrays.asList(
                new Order(1L, "beer#8", 3),
                new Order(1L, "diaper#36", 4),
                new Order(3L, "rubber#35", 2)
        ));

        Table tableA = tableEnv.fromDataStream(sourceA, "user, product, amount");

        // 注册tableFunction
        tableEnv.registerFunction("splitTVF", new SplitTableFunctionRow("#"));

        String sql = "SELECT product, word, length FROM " + tableA + "  , LATERAL TABLE(splitTVF(product)) as T(word, length)  ";

//        String sql = "SELECT product, word, length FROM " + tableA + " LEFT JOIN LATERAL TABLE(splitTVF(product)) as T(word, length) ON TRUE";

        Table result = tableEnv.sqlQuery(sql);

        DataStream<Row> rowDataStream = tableEnv.toAppendStream(result, Row.class);

        rowDataStream.print();

        env.execute("job");
    }

    /**
     * 对某一个字段进行分割，并返回对应的两个列
     * beer#8   =>  beer, 8
     * return Tuple
     */

    public static class SplitTableFunction extends TableFunction<Tuple2<String, String>> {
        private String separator = " ";

        public SplitTableFunction(String separator) {
            this.separator = separator;
        }
        public void eval(String str) {
            String[] split = str.split(separator);
            collect(new Tuple2<String, String>(split[0], split[1]));
        }
    }

    /**
     * 对某一个字段进行分割，并返回对应的两个列
     * beer#8   =>  beer, 8
     * return Row
     */

    public static class SplitTableFunctionRow extends TableFunction<Row> {
        private String separator = " ";

        public SplitTableFunctionRow(String separator) {
            this.separator = separator;
        }
        public void eval(String str) {
            String[] split = str.split(separator);
            Row row = new Row(2);
            row.setField(0, split[0]);
            row.setField(1, split[1]);
            collect(row);
        }

        // 指定返回类型
        @Override
        public TypeInformation<Row> getResultType() {
            return Types.ROW(Types.STRING, Types.STRING);
        }
    }



    /**
     * Simple POJO.
     */
    public static class Order {
        public Long user;
        public String product;
        public int amount;

        public Order() {
        }

        public Order(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "user=" + user +
                    ", product='" + product + '\'' +
                    ", amount=" + amount +
                    '}';
        }
    }
}

