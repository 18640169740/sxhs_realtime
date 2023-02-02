package com.sxhs.realtime.util;

import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @Description: 任务相关工具类
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/31 11:08
 */
public class JobUtils {

    /**
     * 通过表名获取StarrocksSink
     * @param tableName
     * @return
     */
    public static SinkFunction<String> getStarrocksSink(String tableName){
        SinkFunction<String> sink = StarRocksSink.sink(
                StarRocksSinkOptions.builder()
                        .withProperty("jdbc-url", "jdbc:mysql://10.17.41.138:9030?nuc_db")
                        .withProperty("load-url", "10.17.41.138:8030")
                        .withProperty("database-name", "nuc_db")
                        .withProperty("username", "zhangjunwei")
                        .withProperty("password", "Q9yt8fVjdyBq6n$d")
                        .withProperty("table-name", tableName)
                        .withProperty("sink.properties.format", "json")
                        .withProperty("sink.properties.strip_outer_array", "true")
                        .withProperty("sink.semantic", "exactly-once")
                        .build()
        );
        return sink;
    }
}
