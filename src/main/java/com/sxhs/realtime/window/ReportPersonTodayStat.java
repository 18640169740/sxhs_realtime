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
import com.sxhs.realtime.util.UploadNumberUdf;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ReportPersonTodayStat {
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

        tEnv.createTemporarySystemFunction("upload_number", UploadNumberUdf.class);
        Table statTable = _statData(tEnv);
        DataStream<Row> statStream = tEnv.toAppendStream(statTable, Row.class);
        SingleOutputStreamOperator<String> resultStream = statStream.keyBy(row -> row.getField(0))
                .map(new RichMapFunction<Row, String>() {
                    private MapState<String, JSONObject> mapState;
                    private ValueState<JSONObject> valueState;
                    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("ReportPersonTodayStatMapValue", JSONObject.class);
                        MapStateDescriptor<String, JSONObject> mapStateDescriptor = new MapStateDescriptor<>("ReportPersonTodayStatMap", String.class, JSONObject.class);

                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(24))
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();
                        valueStateDescriptor.enableTimeToLive(ttlConfig);
                        mapStateDescriptor.enableTimeToLive(ttlConfig);

                        valueState = getRuntimeContext().getState(valueStateDescriptor);
                        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
                    }

                    @Override
                    public String map(Row row) throws Exception {
                        JSONObject cache = valueState.value();
                        JSONObject result = new JSONObject();
                        if (cache == null) {
                            cache = new JSONObject();
                            result.put("id", SnowflakeIdWorker.generateId());
                            result.put("create_time", sdf.format(new Date()));
                        } else {
                            result.put("id", cache.getLongValue("id"));
                            result.put("create_time", cache.getString("create_time"));

                        }
                        result.put("area_id", row.getField(1));
                        result.put("number_report", null);
                        result.put("source", row.getField(2));
                        result.put("upload_number", (int) row.getField(3) + cache.getIntValue("upload_number"));
                        long successNumber = (long) row.getField(4);
                        result.put("success_number", successNumber + cache.getIntValue("success_number"));
                        long failNumber = (long) row.getField(5);
                        result.put("fail_number", failNumber + cache.getIntValue("fail_number"));
                        result.put("data_file", null);
                        String uploadTime = (String) row.getField(6);
                        result.put("upload_time", uploadTime);
                        result.put("upload_result", 1);
                        result.put("create_by", row.getField(7));
                        result.put("is_delete", 0);
                        result.put("tube_number", (long) row.getField(8) + cache.getLongValue("tube_number"));
                        result.put("drop_data_num", row.getField(9));
                        result.put("upload_all", null);

                        String hour = uploadTime.substring(0, 13);
                        JSONObject mapCache = mapState.get(hour);
                        long uploadHourNum, failHourNum;
                        if (mapCache == null) {
                            mapCache = new JSONObject();
                            uploadHourNum = successNumber;
                            failHourNum = failNumber;
                        } else {
                            uploadHourNum = successNumber + mapCache.getIntValue("upload_hour_num");
                            failHourNum = failNumber + mapCache.getIntValue("fail_hour_num");
                        }
                        result.put("upload_hour_num", uploadHourNum);
                        result.put("fail_hour_num", failHourNum);

                        valueState.update(result);
                        mapCache.put("upload_hour_num", uploadHourNum);
                        mapCache.put("fail_hour_num", failHourNum);
                        mapState.put(hour, mapCache);

                        return result.toJSONString();
                    }
                });
        resultStream.print(">>>");
        SinkFunction<String> srSink = StarRocksSink.sink(
                StarRocksSinkOptions.builder()
                        .withProperty("jdbc-url", "jdbc:mysql://10.17.41.138:9030?nuc_db")
                        .withProperty("load-url", "10.17.41.138:8030")
                        .withProperty("database-name", "nuc_db")
                        .withProperty("username", "huquan")
                        .withProperty("password", "oNa46nj0o65b@kvK")
                        .withProperty("table-name", "upload_log_time")
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

    private static Table _statData(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery("select " +
                "concat_ws('_',cast(areaId as string),'1',SUBSTRING(max(interfaceRecTime) from 1 FOR 10)) union_id, " +
                "areaId area_id, " +
                "1 source, " +
                "upload_number(concat_ws('_',personIdCard,tubeCode)) upload_number, " +
                "count(1) success_number, " +
                "count(distinct personIdCard) fail_number, " +
                "max(interfaceRecTime) upload_time, " +
                "max(userName) create_by, " +
                "count(distinct tubeCode) tube_number, " +
                "max(dropDataNum) drop_data_num " +
                "from collect_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' SECOND), " +
                "areaId " +
                "union all " +
                "select " +
                "concat_ws('_',cast(areaId as string),'2',SUBSTRING(max(interfaceRecTime) from 1 FOR 10)) union_id, " +
                "areaId area_id, " +
                "2 source, " +
                "0 upload_number, " +
                "count(1) success_number, " +
                "0 fail_number, " +
                "max(interfaceRecTime) upload_time, " +
                "max(userName) create_by, " +
                "sum(tubeNum) tube_number, " +
                "max(dropDataNum) drop_data_num " +
                "from transport_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' SECOND), " +
                "areaId " +
                "union all " +
                "select " +
                "concat_ws('_',cast(areaId as string),'3',SUBSTRING(max(interfaceRecTime) from 1 FOR 10)) union_id, " +
                "areaId area_id, " +
                "3 source, " +
                "0 upload_number, " +
                "count(1) success_number, " +
                "0 fail_number, " +
                "max(interfaceRecTime) upload_time, " +
                "max(userName) create_by, " +
                "sum(tubeNum) tube_number, " +
                "max(dropDataNum) drop_data_num " +
                "from receive_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' SECOND), " +
                "areaId " +
                "union all " +
                "select " +
                "concat_ws('_',cast(areaId as string),'4',SUBSTRING(max(interfaceRecTime) from 1 FOR 10)) union_id, " +
                "areaId area_id, " +
                "4 source, " +
                "upload_number(concat_ws('_',personIdCard,tubeCode)) upload_number, " +
                "count(1) success_number, " +
                "count(distinct personIdCard) fail_number, " +
                "max(interfaceRecTime) upload_time, " +
                "max(userName) create_by, " +
                "count(distinct tubeCode) tube_number, " +
                "max(dropDataNum) drop_data_num " +
                "from report_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' SECOND), " +
                "areaId ");
    }
}
