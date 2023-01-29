package com.sxhs.realtime.window;

import com.alibaba.fastjson.JSONObject;
import com.sxhs.realtime.bean.CollectDataId;
import com.sxhs.realtime.bean.ReceiveDataId;
import com.sxhs.realtime.bean.ReportDataId;
import com.sxhs.realtime.bean.TransportDataId;
import com.sxhs.realtime.util.StreamUtil;
import com.sxhs.realtime.util.TableUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

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
            // TODO id
            jsonObj.put("id", 123l);
            jsonObj.put("area_id", row.getField(0));
            jsonObj.put("source", row.getField(1));
            jsonObj.put("day_date", row.getField(2));
            jsonObj.put("unit_hour", row.getField(3));
            jsonObj.put("delay_avg", row.getField(4));
            jsonObj.put("create_time", row.getField(5));
            jsonObj.put("unit_minute", row.getField(6));
            return jsonObj.toJSONString();
        });
        resultStream.print();
//        SingleOutputStreamOperator<String> resultStream = unionStream.keyBy(row -> row.getField(0))
//                .map(new RichMapFunction<Row, String>() {
//                    private ValueState<JSONObject> cacheState;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("CollectAndReportTimeDifStat", JSONObject.class);
//                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(24))
//                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
//                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//                                .build();
//                        valueStateDescriptor.enableTimeToLive(ttlConfig);
//                        cacheState = getRuntimeContext().getState(valueStateDescriptor);
//                    }
//
//                    @Override
//                    public String map(Row row) throws Exception {
//                        JSONObject cache = cacheState.value();
//                        JSONObject jsonObj = new JSONObject();
//                        long id, difCount;
//                        String createTime;
//                        int difTime;
//                        if (cache == null) {
//                            // TODO 生成id
//                            id = 123456l;
//                            difTime = (int) row.getField(5);
//                            difCount = (long) row.getField(6);
//                            createTime = (String) row.getField(7);
//                        } else {
//                            id = cache.getLongValue("id");
//                            difTime = (int) row.getField(5) + cache.getIntValue("dif_time");
//                            difCount = (long) row.getField(6) + cache.getIntValue("dif_count");
//                            createTime = cache.getString("create_time");
//                        }
//                        jsonObj.put("id", id);
//                        jsonObj.put("area_id", row.getField(1));
//                        jsonObj.put("source", row.getField(2));
//                        jsonObj.put("day_date", row.getField(3));
//                        jsonObj.put("unit_hour", row.getField(4));
//                        jsonObj.put("create_time", createTime);
//                        jsonObj.put("unit_minute", row.getField(8));
//
//                        // 更新cache。cache中不直接保存avg，只保存dif_time和dif_count
//                        cache = JSONObject.parseObject(jsonObj.toJSONString());
//                        cache.put("dif_time", difTime);
//                        cache.put("dif_count", difCount);
//                        cacheState.update(cache);
//
//                        jsonObj.put("delay_avg", difTime / difCount);
//                        return jsonObj.toJSONString();
//                    }
//                });

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
