package com.sxhs.realtime.window;

import com.alibaba.fastjson.JSONObject;
import com.sxhs.realtime.bean.CollectDataId;
import com.sxhs.realtime.bean.ReceiveDataId;
import com.sxhs.realtime.bean.ReportDataId;
import com.sxhs.realtime.bean.TransportDataId;
import com.sxhs.realtime.common.BaseJob;
import com.sxhs.realtime.util.*;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

// 小时维度上报人次统计任务
public class ReportPersonCountPerHourStat extends BaseJob {
    //    private static final String TUMBLE_STR = "TUMBLE(pt, INTERVAL '3' MINUTE), ";
    private static final String TUMBLE_STR = "TUMBLE(pt, INTERVAL '1' SECOND), ";

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = new String[]{
                    "--bootstrap.servers", "10.17.41.132:9091,10.17.41.133:9091,10.17.41.134:9091",
                    "--group.id", "huquan_ReportPersonCountPerHourStat",
                    "--source.topic.name", "NUC_DATA_ID"
            };
        }
        //参数解析
        final ParameterTool parameterToolold = ParameterTool.fromArgs(args);
        Map<String, String> parameterMap = new HashMap<>(parameterToolold.toMap());
        //设置全局参数
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(parameterMap));
        //状态后端采用RocksDB/增量快照
        env.setStateBackend(new RocksDBStateBackend("hdfs://NSBD/warehouse_bigdata/realtimecompute/rtcalc/huquan_ReportPersonCountPerHourStat", true));
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
        Table reportDataTable = TableUtil.reportTable(tEnv, streams.f3);

        // 建表
        tEnv.createTemporaryView("collect_data", collectDataTable);
        tEnv.createTemporaryView("report_data", reportDataTable);

        tEnv.createTemporarySystemFunction("check_persons", UploadNumberUdf.class);

        Table collectStat = _statCollect(tEnv);
        Table reportState = _statReport(tEnv);

        Table unionTable = collectStat.unionAll(reportState);
        DataStream<Row> unionStream = tEnv.toAppendStream(unionTable, Row.class);

        SingleOutputStreamOperator<String> resultStream = unionStream.keyBy(row -> row.getField(0)).map(new RichMapFunction<Row, String>() {
            private ValueState<JSONObject> cacheState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("ReportPersonCountPerHour", JSONObject.class);
                StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(48))
                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .build();
                valueStateDescriptor.enableTimeToLive(ttlConfig);
                cacheState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public String map(Row row) throws Exception {
                JSONObject result = new JSONObject();
                JSONObject cache = cacheState.value();
                if (cache == null) {
                    cache = new JSONObject();
                    cache.put("id", SnowflakeIdWorker.generateId());
                    cache.put("create_time", row.getField(7));
                }
                result.put("id", cache.getLongValue("id"));
                result.put("area_id", row.getField(1));
                result.put("unit_hour", row.getField(2));
                result.put("collect_persons", (long) row.getField(3) + cache.getLongValue("collect_persons"));
                result.put("transfer_persons", 0);
                result.put("receive_persons", 0);
                int checkPersons = (int) row.getField(4) + cache.getIntValue("check_persons");
                result.put("check_persons", checkPersons);
                result.put("create_time", cache.getString("create_time"));
                result.put("is_delete", 0);
                result.put("check_numbers", checkPersons);
                result.put("collect_all", (long) row.getField(5) + cache.getLongValue("collect_all"));
                result.put("check_all", (long) row.getField(6) + cache.getLongValue("check_all"));
                cacheState.update(result);
                return result.toJSONString();
            }
        });
        SinkFunction<String> srSink = JobUtils.getStarrocksSink("upload_log_hour");
        resultStream.addSink(srSink);
        env.execute("huquan_ReportPersonCountPerHourStat");
    }

    private static Table _statReport(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery("select " +
                "concat_ws('_',cast(areaId as string),cast(hour(TO_TIMESTAMP(collectTime)) as string),substring(max(interfaceRecTime) from 0 for 10)) union_id, " +
                "areaId area_id, " +
                "hour(TO_TIMESTAMP(collectTime)) unit_hour, " +
                "cast(0 as bigint) collect_persons, " +
                "check_persons(distinct concat_ws('_',personIdCard,tubeCode)) check_persons, " +
                "0 collect_all, " +
                "count(1) check_all, " +
                "max(interfaceRecTime) create_time " +
                "from report_data " +
                "group by " +
                TUMBLE_STR +
                "areaId, " +
                "hour(TO_TIMESTAMP(collectTime))");
    }

    private static Table _statCollect(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery("select " +
                // 为union_id添加日期字段，取max(interfaceRecTime)
                "concat_ws('_',cast(areaId as string),cast(hour(TO_TIMESTAMP(collectTime)) as string),substring(max(interfaceRecTime) from 0 for 10)) union_id, " +
                "areaId area_id, " +
                "hour(TO_TIMESTAMP(collectTime)) unit_hour, " +
                "count(distinct personIdCard) collect_persons, " +
                "0 check_persons, " +
                "count(1) collect_all, " +
                "0 check_all, " +
                "max(interfaceRecTime) create_time " +
                "from collect_data " +
                "group by " +
                TUMBLE_STR +
                "areaId, " +
                "hour(TO_TIMESTAMP(collectTime))");
    }
}
