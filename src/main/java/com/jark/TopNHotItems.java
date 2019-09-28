package com.jark;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/***
 *  求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串
 *  */
public class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

    // top N
    private final int topSize;

    private ListState<ItemViewCount> itemState;

    public TopNHotItems(int topSize) {
        this.topSize = topSize;
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 获取收到的所有商品点击量
        List<ItemViewCount> allItems = new ArrayList<ItemViewCount>();
        for(ItemViewCount item: itemState.get()){
            allItems.add(item);
        }

        // 提前清除状态中的数据，释放空间
        itemState.clear();
        // 按照点击量从大到小排序
        allItems.sort(new Comparator<ItemViewCount>() {
            @Override
            public int compare(ItemViewCount o1, ItemViewCount o2) {
                return (int) (o2.getViewCount() - o1.getViewCount());
            }
        });

        // 将排名信息格式化成 String, 便于打印
        StringBuilder result = new StringBuilder();
        result.append("====================================\n");
        result.append("时间: ").append(new Timestamp(timestamp-1)).append("\n");
        for (int i = 0; i < allItems.size() && i < topSize; i++) {
            ItemViewCount currentItem = allItems.get(i);
            result.append("No").append(i).append(": ")
                    .append("商品ID=").append(currentItem.getItemId()).append("\t")
                    .append("浏览量=").append(currentItem.getViewCount())
                    .append("\n");

        }
        result.append("====================================\n\n");

        // 控制输出频率，模拟实时滚动结果
        Thread.sleep(1000);

        out.collect(result.toString());
    }

    /**
     * 获取状态
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<ItemViewCount> descriptor = new ListStateDescriptor<>
                ("itemState", ItemViewCount.class);
        itemState = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void processElement(ItemViewCount input, Context ctx, Collector<String> out) throws Exception {
        // 将数据保存到状态中
        itemState.add(input);
        // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
        ctx.timerService().registerEventTimeTimer(input.getWindowEnd() + 1);
    }
}
