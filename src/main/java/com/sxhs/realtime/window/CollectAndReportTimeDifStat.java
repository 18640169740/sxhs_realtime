package com.sxhs.realtime.window;

import com.alibaba.fastjson.JSONObject;
import com.sxhs.realtime.bean.CollectDataId;
import com.sxhs.realtime.bean.ReceiveDataId;
import com.sxhs.realtime.bean.ReportDataId;
import com.sxhs.realtime.bean.TransportDataId;
import com.sxhs.realtime.common.BaseJob;
import com.sxhs.realtime.util.JobUtils;
import com.sxhs.realtime.util.SnowflakeIdWorker;
import com.sxhs.realtime.util.StreamUtil;
import com.sxhs.realtime.util.TableUtil;
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

// 采检时间差统计任务
public class CollectAndReportTimeDifStat extends BaseJob {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = new String[]{
                    "--bootstrap.servers", "10.17.41.132:9091,10.17.41.133:9091,10.17.41.134:9091",
                    "--group.id", "huquan_CollectAndReportTimeDifStat",
                    "--source.topic.name", "NUC_DATA_ID"
            };
        }
        //参数解析
        final ParameterTool parameterToolold = ParameterTool.fromArgs(args);
        Map<String, String> parameterMap = new HashMap<>(parameterToolold.toMap());
        //设置全局参数
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(parameterMap));
        //状态后端采用RocksDB/增量快照
        env.setStateBackend(new RocksDBStateBackend("hdfs://NSBD/warehouse_bigdata/realtimecompute/rtcalc/huquan_CollectAndReportTimeDifStat", true));
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

        Tuple4<SingleOutputStreamOperator<CollectDataId>, DataStream<TransportDataId>, DataStream<ReceiveDataId>, DataStream<ReportDataId>> streams = StreamUtil.sideOutput(sourceStream);
        Table reportDataTable = TableUtil.reportTable(tEnv, streams.f3);
        tEnv.createTemporaryView("report_data", reportDataTable);

        Table statTable = tEnv.sqlQuery("select " +
                "concat_ws('_',cast(areaId as string),substring(max(interfaceRecTime) from 0 for 10),cast(TIMESTAMPDIFF(HOUR,TO_TIMESTAMP(collectTime),TO_TIMESTAMP(checkTime)) as string)) union_id, " +
                "areaId area_id, " +
                "TIMESTAMPDIFF(HOUR,TO_TIMESTAMP(collectTime),TO_TIMESTAMP(checkTime)) unit_hour, " +
                "count(personId) check_persons, " +
                "max(interfaceRecTime) interfaceRecTime " +
                "from report_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' MINUTE), " +
                "TIMESTAMPDIFF(HOUR,TO_TIMESTAMP(collectTime),TO_TIMESTAMP(checkTime)), " +
                "areaId");
        DataStream<Row> statStream = tEnv.toAppendStream(statTable, Row.class);
        SingleOutputStreamOperator<String> resultStream = statStream.keyBy(row -> row.getField(0))
                .map(new RichMapFunction<Row, String>() {
                    private ValueState<JSONObject> cacheState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("CollectAndReportTimeDifStat", JSONObject.class);
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(48))
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();
                        valueStateDescriptor.enableTimeToLive(ttlConfig);
                        cacheState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public String map(Row row) throws Exception {
                        JSONObject cache = cacheState.value();
                        JSONObject jsonObj = new JSONObject();
                        jsonObj.put("area_id", row.getField(1));
                        jsonObj.put("unit_hour", row.getField(2));
                        if (cache == null) {
                            cache = new JSONObject();
                            jsonObj.put("id", SnowflakeIdWorker.generateId());
                            jsonObj.put("create_time", row.getField(4));
                        } else {
                            jsonObj.put("id", cache.getLong("id"));
                            jsonObj.put("create_time", cache.getString("create_time"));
                        }
                        jsonObj.put("check_persons", (long) row.getField(3) + cache.getLongValue("check_persons"));
                        cacheState.update(jsonObj);
                        jsonObj.put("is_delete", 0);
                        return jsonObj.toJSONString();
                    }
                });


        SinkFunction<String> srSink = JobUtils.getStarrocksSink("upload_log_checkhour");
        resultStream.addSink(srSink);
        env.execute("huquan_CollectAndReportTimeDifStat");
    }
}
