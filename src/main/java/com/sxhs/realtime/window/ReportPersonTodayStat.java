package com.sxhs.realtime.window;

import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.sxhs.realtime.bean.CollectDataId;
import com.sxhs.realtime.bean.ReceiveDataId;
import com.sxhs.realtime.bean.ReportDataId;
import com.sxhs.realtime.bean.TransportDataId;
import com.sxhs.realtime.common.BaseJob;
import com.sxhs.realtime.util.*;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

//3.3.3.2.6当天内记录的上报与人次统计任务
public class ReportPersonTodayStat extends BaseJob {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = new String[]{
                    "--bootstrap.servers", "10.17.41.132:9091,10.17.41.133:9091,10.17.41.134:9091",
                    "--group.id", "huquan_ReportPersonTodayStat",
                    "--source.topic.name", "NUC_DATA_ID"
            };
        }
        //参数解析
        final ParameterTool parameterToolold = ParameterTool.fromArgs(args);
        Map<String, String> parameterMap = new HashMap<>(parameterToolold.toMap());
        //设置全局参数
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(parameterMap));
        //状态后端采用RocksDB/增量快照
        env.setStateBackend(new RocksDBStateBackend("hdfs://NSBD/warehouse_bigdata/realtimecompute/rtcalc/huquan_ReportPersonTodayStat", true));
        env.getConfig().isUseSnapshotCompression();

        //设置kafka参数
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, parameterMap.get("bootstrap.servers"));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, parameterMap.get("group.id"));
        FlinkKafkaConsumer<String> input = new FlinkKafkaConsumer<>(parameterMap.get("source.topic.name"), new SimpleStringSchema(), properties);
        input.setStartFromGroupOffsets();

        //读取kafka数据
        DataStreamSource<String> sourceStream = env.addSource(input);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

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

                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(48))
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

        SinkFunction<String> srSink = JobUtils.getStarrocksSink("upload_log_time");
        resultStream.addSink(srSink);
        env.execute("huquan_ReportPersonTodayStat");
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
                "TUMBLE(pt, INTERVAL '3' MINUTE), " +
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
                "TUMBLE(pt, INTERVAL '3' MINUTE), " +
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
                "TUMBLE(pt, INTERVAL '3' MINUTE), " +
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
                "TUMBLE(pt, INTERVAL '3' MINUTE), " +
                "areaId ");
    }
}
