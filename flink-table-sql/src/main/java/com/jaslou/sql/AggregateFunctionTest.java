package com.jaslou.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 使用AggregateFunction计算平均值
 */
public class AggregateFunctionTest  {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        env.setParallelism(1);

        DataStream<Order> sourceA = env.fromCollection(Arrays.asList(
                new SubOrder(1L, "beer", 3, 10L, 1),
                new SubOrder(1L, "beer", 3, 15L, 2),
                new SubOrder(1L, "diaper", 4, 20L, 3),
                new SubOrder(1L, "diaper", 4, 20L, 4),
                new SubOrder(3L, "rubber", 2, 30L, 5),
                new SubOrder(3L, "rubber", 2, 15L, 6)
        ));

        Table tableA = tableEnv.fromDataStream(sourceA, "user, product, amount, price, cnt");

        // 注册tableFunction
        tableEnv.registerFunction("wAvg", new WeighedAvgAggregateFunction());

        String sql = "SELECT product, wAvg(price, cnt) AS avgPoints FROM " + tableA + " GROUP BY product";

        // use function
        Table result = tableEnv.sqlQuery(sql);

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(result, Row.class);

        tuple2DataStream.print();

        env.execute("AggregateFunction job!");

    }

    public static class WeightedAvgAccum {
        public long sum = 0;
        public int count = 0;
    }

    /**
     * Weighted Average user-defined aggregate function.
     */
    public static class WeighedAvgAggregateFunction extends AggregateFunction<Long, WeightedAvgAccum> {

        public WeighedAvgAggregateFunction() {
        }

        @Override
        public WeightedAvgAccum createAccumulator() {
            return new WeightedAvgAccum();
        }

        @Override
        public Long getValue(WeightedAvgAccum accumulator) {
            if (accumulator.count == 0) {
                return null;
            }else {
                return accumulator.sum / accumulator.count;
            }
        }

        public void accumulate(WeightedAvgAccum accumulator, long iValue, int iWeight) {
            accumulator.sum += iValue * iWeight;
            accumulator.count += iWeight;
        }

        public void retract(WeightedAvgAccum acc, long iValue, int iWeight) {
            acc.sum -= iValue * iWeight;
            acc.count -= iWeight;
        }

        public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it) {
            Iterator<WeightedAvgAccum> iter = it.iterator();
            while (iter.hasNext()) {
                WeightedAvgAccum a = iter.next();
                acc.count += a.count;
                acc.sum += a.sum;
            }
        }

        public void resetAccumulator(WeightedAvgAccum acc) {
            acc.count = 0;
            acc.sum = 0L;
        }




    }

    public static class SubOrder extends Order {
        public Long price; //单价
        public int cnt; //数量

        public SubOrder(Long user, String product, int amount, Long price, int cnt) {
            super(user, product, amount);
            this.price = price;
            this.cnt = cnt;
        }

        public SubOrder() {
        }
    }
}


