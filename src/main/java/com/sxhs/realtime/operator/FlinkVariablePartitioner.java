package com.sxhs.realtime.operator;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

/**
 * @author zhangJunWei
 * 自定义分区策略
 * @date: 2023/1/28 12:07
 **/
public class FlinkVariablePartitioner<T> extends FlinkKafkaPartitioner<T> {

    private int parallelInstanceId;
    private int index = 0;
    @Override
    public void open(int parallelInstanceId, int parallelInstances) {
        super.open(parallelInstanceId, parallelInstances);
        this.parallelInstanceId = parallelInstanceId;
    }

    @Override
    public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {

        int partitionLength = partitions.length;
        int tempIndex = index;

        if(tempIndex < partitionLength-1){
            index ++;
        }else if(tempIndex == partitionLength-1){
            index = 0;
        }
        return tempIndex;
    }

}
