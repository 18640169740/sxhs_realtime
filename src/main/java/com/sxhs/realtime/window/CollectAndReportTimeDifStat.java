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
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.text.SimpleDateFormat;
import java.util.Properties;

// 采检时间差统计任务
public class CollectAndReportTimeDifStat {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // TODO 删除测试代码
        Properties properties = new Properties();
//        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.17.41.132:9092,10.17.41.133:9092,10.17.41.134:9092");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-huq");
//        FlinkKafkaConsumer<String> input = new FlinkKafkaConsumer<>("NUC_DATA_ID", new SimpleStringSchema(), properties);
        FlinkKafkaConsumer<String> input = new FlinkKafkaConsumer<>("sxhs_NUC_DATA_ID", new SimpleStringSchema(), properties);
        input.setStartFromEarliest();
        DataStreamSource<String> sourceStream = env.addSource(input);
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
                "TUMBLE(pt, INTERVAL '3' SECOND), " +
                "TIMESTAMPDIFF(HOUR,TO_TIMESTAMP(collectTime),TO_TIMESTAMP(checkTime)), " +
                "areaId");
        DataStream<Row> statStream = tEnv.toAppendStream(statTable, Row.class);
        SingleOutputStreamOperator<String> resultStream = statStream.keyBy(row -> row.getField(0))
                .map(new RichMapFunction<Row, String>() {
                    private ValueState<JSONObject> cacheState;
                    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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

        SinkFunction<String> srSink = StarRocksSink.sink(
                StarRocksSinkOptions.builder()
                        .withProperty("jdbc-url", "jdbc:mysql://10.17.41.138:9030?nuc_db")
                        .withProperty("load-url", "10.17.41.138:8030")
                        .withProperty("database-name", "nuc_db")
                        .withProperty("username", "huquan")
                        .withProperty("password", "oNa46nj0o65b@kvK")
                        .withProperty("table-name", "upload_log_checkhour")
                        .withProperty("sink.properties.format", "json")
                        .withProperty("sink.properties.strip_outer_array", "true")
                        // TODO 删除测试代码
                        .withProperty("sink.buffer-flush.interval-ms", "1000")
                        // 设置并行度，多并行度情况下需要考虑如何保证数据有序性
                        .withProperty("sink.parallelism", "1")
                        .build()
        );
        resultStream.addSink(srSink);
        resultStream.print("result");
        env.execute("huquan_CollectAndReportTimeDifStat");
    }
}
