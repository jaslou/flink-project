package com.jark;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/** 用于输出窗口的结果 */
public class ResultWindowFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
    @Override
    public void apply(Tuple key,
                      TimeWindow window,
                      Iterable<Long> input,
                      Collector<ItemViewCount> out) {
        Long itemId = ((Tuple1<Long>)key).f0;
        Long windowEnd = window.getEnd();
        Long count = input.iterator().next();
        out.collect(ItemViewCount.of(itemId, windowEnd, count));
    }
}
