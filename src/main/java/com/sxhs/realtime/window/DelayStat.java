package com.sxhs.realtime.window;

import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.sxhs.realtime.bean.CollectDataId;
import com.sxhs.realtime.bean.ReceiveDataId;
import com.sxhs.realtime.bean.ReportDataId;
import com.sxhs.realtime.bean.TransportDataId;
import com.sxhs.realtime.util.SnowflakeIdWorker;
import com.sxhs.realtime.util.StreamUtil;
import com.sxhs.realtime.util.TableUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

// 3.3.3.2.5各环节延迟统计任务
public class DelayStat {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> sourceStream = env.socketTextStream("localhost", 7777);

        // 侧输出流，将kafka数据分离，collect、transport、receive、report
        Tuple4<SingleOutputStreamOperator<CollectDataId>, DataStream<TransportDataId>, DataStream<ReceiveDataId>, DataStream<ReportDataId>> streams = StreamUtil.sideOutput(sourceStream);

        // stream转table
        Table collectDataTable = TableUtil.collectTable(tEnv, streams.f0);
        Table transportDataTable = TableUtil.transportTable(tEnv, streams.f1);
        Table receiveDataTable = TableUtil.receiveTable(tEnv, streams.f2);
        Table reportDataTable = TableUtil.reportTable(tEnv, streams.f3);

        // 建表
        tEnv.createTemporaryView("collect_data", collectDataTable);
        tEnv.createTemporaryView("transport_data", transportDataTable);
        tEnv.createTemporaryView("receive_data", receiveDataTable);
        tEnv.createTemporaryView("report_data", reportDataTable);

        Table collectStat = _statCollect(tEnv);
        Table transportStat = _statTransport(tEnv);
        Table receiveState = _statReceive(tEnv);
        Table reportState = _statReport(tEnv);

        Table unionTable = collectStat
                .unionAll(transportStat)
                .unionAll(receiveState)
                .unionAll(reportState);
        DataStream<Row> unionStream = tEnv.toAppendStream(unionTable, Row.class);
        SingleOutputStreamOperator<String> resultStream = unionStream.map((MapFunction<Row, String>) row -> {
            JSONObject jsonObj = new JSONObject();
            jsonObj.put("id", SnowflakeIdWorker.generateId());
            jsonObj.put("area_id", row.getField(0));
            jsonObj.put("source", row.getField(1));
            jsonObj.put("day_date", row.getField(2));
            jsonObj.put("unit_hour", row.getField(3));
            jsonObj.put("delay_avg", row.getField(4));
            jsonObj.put("create_time", row.getField(5));
            jsonObj.put("unit_minute", row.getField(6));
            return jsonObj.toJSONString();
        });

        SinkFunction<String> srSink = StarRocksSink.sink(
                StarRocksSinkOptions.builder()
                        .withProperty("jdbc-url", "jdbc:mysql://10.17.41.138:9030?nuc_db")
                        .withProperty("load-url", "10.17.41.138:8030")
                        .withProperty("database-name", "nuc_db")
                        .withProperty("username", "huquan")
                        .withProperty("password", "oNa46nj0o65b@kvK")
                        .withProperty("table-name", "delay_avg")
                        .withProperty("sink.properties.format", "json")
                        .withProperty("sink.properties.strip_outer_array", "true")
                        // TODO 删除测试代码
                        .withProperty("sink.buffer-flush.interval-ms", "1000")
                        // 设置并行度，多并行度情况下需要考虑如何保证数据有序性
                        .withProperty("sink.parallelism", "1")
                        .build()
        );
        resultStream.addSink(srSink);
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private static Table _statReport(StreamTableEnvironment tEnv) {
        String sql = "select " +
//                "concat_ws('_',cast(areaId as string),'1',SUBSTRING(checkTime from 1 FOR 10),cast(HOUR(TIMESTAMPADD(HOUR, 8,TUMBLE_START(pt,INTERVAL '10' SECOND))) as string),cast(MINUTE(TUMBLE_START(pt,INTERVAL '10' SECOND)) as string)) union_id, " +
                "areaId area_id," +
                "4 source, " +
                "SUBSTRING(checkTime from 1 FOR 10) day_date, " +
                "HOUR(TIMESTAMPADD(HOUR, 8,TUMBLE_START(pt,INTERVAL '10' SECOND))) unit_hour, " +
                "avg(timestampdiff(minute, TO_TIMESTAMP(checkTime), TO_TIMESTAMP(interfaceRecTime))) delay_avg, " +
                // TODO 可以优化为当前时间
                "max(interfaceRecTime) create_time, " +
                "MINUTE(TUMBLE_START(pt,INTERVAL '10' SECOND)) unit_minute " +
                "from report_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '10' SECOND), " +
                // 按处理时间开床10分钟，窗口内数据的unit_hour，unit_minute相同，无需再分组
                "areaId,checkTime";
        return tEnv.sqlQuery(sql);
    }

    private static Table _statReceive(StreamTableEnvironment tEnv) {
        String sql = "select " +
//                "concat_ws('_',cast(areaId as string),'1',SUBSTRING(receiveTime from 1 FOR 10),cast(HOUR(TIMESTAMPADD(HOUR, 8,TUMBLE_START(pt,INTERVAL '10' SECOND))) as string),cast(MINUTE(TUMBLE_START(pt,INTERVAL '10' SECOND)) as string)) union_id, " +
                "areaId area_id," +
                "3 source, " +
                "SUBSTRING(receiveTime from 1 FOR 10) day_date, " +
                "HOUR(TIMESTAMPADD(HOUR, 8,TUMBLE_START(pt,INTERVAL '10' SECOND))) unit_hour, " +
                "avg(timestampdiff(minute, TO_TIMESTAMP(receiveTime), TO_TIMESTAMP(interfaceRecTime))) delay_avg, " +
                "max(interfaceRecTime) create_time, " +
                "MINUTE(TUMBLE_START(pt,INTERVAL '10' SECOND)) unit_minute " +
                "from receive_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '10' SECOND), " +
                // 按处理时间开床10分钟，窗口内数据的unit_hour，unit_minute相同，无需再分组
                "areaId,receiveTime";
        return tEnv.sqlQuery(sql);
    }

    private static Table _statTransport(StreamTableEnvironment tEnv) {
        // TODO 暂取deliveryTime（交付时间）
        String sql = "select " +
//                "concat_ws('_',cast(areaId as string),'1',SUBSTRING(deliveryTime from 1 FOR 10),cast(HOUR(TIMESTAMPADD(HOUR, 8,TUMBLE_START(pt,INTERVAL '10' SECOND))) as string),cast(MINUTE(TUMBLE_START(pt,INTERVAL '10' SECOND)) as string)) union_id, " +
                "areaId area_id," +
                "2 source, " +
                "SUBSTRING(deliveryTime from 1 FOR 10) day_date, " +
                "HOUR(TIMESTAMPADD(HOUR, 8,TUMBLE_START(pt,INTERVAL '10' SECOND))) unit_hour, " +
                "avg(timestampdiff(minute, TO_TIMESTAMP(deliveryTime), TO_TIMESTAMP(interfaceRecTime))) delay_avg, " +
                "max(interfaceRecTime) create_time, " +
                "MINUTE(TUMBLE_START(pt,INTERVAL '10' SECOND)) unit_minute " +
                "from transport_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '10' SECOND), " +
                // 按处理时间开床10分钟，窗口内数据的unit_hour，unit_minute相同，无需再分组
                "areaId,deliveryTime";
        return tEnv.sqlQuery(sql);
    }

    private static Table _statCollect(StreamTableEnvironment tEnv) {
        String sql = "select " +
//                "concat_ws('_',cast(areaId as string),'1',SUBSTRING(collectTime from 1 FOR 10),cast(HOUR(TIMESTAMPADD(HOUR, 8,TUMBLE_START(pt,INTERVAL '10' SECOND))) as string),cast(MINUTE(TUMBLE_START(pt,INTERVAL '10' SECOND)) as string)) union_id, " +
                "areaId area_id," +
                "1 source, " +
                "SUBSTRING(collectTime from 1 FOR 10) day_date, " +
                "HOUR(TIMESTAMPADD(HOUR, 8,TUMBLE_START(pt,INTERVAL '10' SECOND))) unit_hour, " +
                "avg(timestampdiff(minute, TO_TIMESTAMP(collectTime), TO_TIMESTAMP(interfaceRecTime))) delay_avg, " +
                "max(interfaceRecTime) create_time, " +
                "MINUTE(TUMBLE_START(pt,INTERVAL '10' SECOND)) unit_minute " +
                "from collect_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '10' SECOND), " +
                // 按处理时间开床10分钟，窗口内数据的unit_hour，unit_minute相同，无需再分组
                "areaId,collectTime";
        return tEnv.sqlQuery(sql);
    }
}
