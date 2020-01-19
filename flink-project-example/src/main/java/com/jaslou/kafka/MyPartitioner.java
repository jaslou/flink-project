package com.jaslou.kafka;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.util.Preconditions;

public class MyPartitioner extends FlinkKafkaPartitioner {

    private int parallelInstanceId;

    public MyPartitioner() {
        super();
    }

    @Override
    public void open(int parallelInstanceId, int parallelInstances) {
        Preconditions.checkArgument(parallelInstanceId >= 0, "Id of this subtask cannot be negative.");
        Preconditions.checkArgument(parallelInstances > 0, "Number of subtasks must be larger than 0.");
        this.parallelInstanceId = parallelInstanceId;
    }

    @Override
    public int partition(Object record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        //这里接收到的key是上面MySchema()中序列化后的key，需要转成string，然后取key的hash值`%`上kafka分区数量
//        ((UserBehavior)record).userId
//        return Math.abs(new String(key).hashCode() % partitions.length);
        return partitions[parallelInstanceId % partitions.length];
    }
}
