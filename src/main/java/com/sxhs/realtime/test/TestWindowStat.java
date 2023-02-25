package com.sxhs.realtime.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.sxhs.realtime.util.JobUtils;
import com.sxhs.realtime.util.SnowflakeIdWorker;
import com.sxhs.realtime.util.UploadNumberUdf;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
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
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.text.DecimalFormat;
import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

public class TestWindowStat {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.17.41.132:9091,10.17.41.133:9091,10.17.41.134:9091");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-huq");
        FlinkKafkaConsumer<String> input = new FlinkKafkaConsumer<>("NUC_DATA_ZJW", new SimpleStringSchema(), properties);
//        input.setStartFromGroupOffsets();
        input.setStartFromLatest();
//        input.setStartFromEarliest();

        DataStreamSource<String> kafkaStream = env.addSource(input);
        SingleOutputStreamOperator<TestReportData> reportS = kafkaStream
                .flatMap(new FlatMapFunction<String, TestReportData>() {
                    @Override
                    public void flatMap(String s, Collector<TestReportData> collector) throws Exception {
                        try {
                            JSONObject jsonObj = JSONObject.parseObject(s);
                            if ("REPORT_DATA".equals(jsonObj.getString("type"))) {
                                _toReport(jsonObj, collector);
                            }
                        } catch (Exception ignored) {
                        }
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<TestReportData>forBoundedOutOfOrderness(Duration.ofSeconds(1000))
                        .withTimestampAssigner(new SerializableTimestampAssigner<TestReportData>() {
                            @Override
                            public long extractTimestamp(TestReportData report, long recordTimestamp) {
                                return report.getTimestamp();
                            }
                        }));
        Table reportT = tEnv.fromDataStream(reportS,
                $("areaId"),
                $("personId"),
                $("collectTime"),
                $("checkTime"),
                $("interfaceRecTime"),
                $("timestamp"),
                $("pt").proctime(),
                $("rt").rowtime());
        tEnv.createTemporaryView("report_data", reportT);

        tEnv.createTemporarySystemFunction("mill_dif", DiffUdf.class);

        Table statTable = tEnv.sqlQuery("select " +
                "concat_ws('_',cast(areaId as string),substring(max(interfaceRecTime) from 0 for 10),cast(TIMESTAMPDIFF(HOUR,TO_TIMESTAMP(collectTime),TO_TIMESTAMP(checkTime)) as string)) union_id, " +
                "areaId area_id, " +
                "TIMESTAMPDIFF(HOUR,TO_TIMESTAMP(collectTime),TO_TIMESTAMP(checkTime)) unit_hour, " +
                "count(personId) check_persons, " +
                "max(interfaceRecTime) interfaceRecTime, " +
                "sum(mill_dif(rt,pt)) sum_delay, " +
                "count(1) total " +
                "from report_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' MINUTE), " +
                "TIMESTAMPDIFF(HOUR,TO_TIMESTAMP(collectTime),TO_TIMESTAMP(checkTime)), " +
                "areaId");
        DataStream<Row> statStream = tEnv.toAppendStream(statTable, Row.class);
        SingleOutputStreamOperator<String> resultS = statStream.keyBy(row -> row.getField(0))
                .map(new RichMapFunction<Row, String>() {
                    private ValueState<JSONObject> cacheState;
                    private DecimalFormat df = new DecimalFormat("#.00");


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

                        long sumDelay = (long) row.getField(5) + cache.getLongValue("sum_delay");
                        long total = (long) row.getField(6) + cache.getLongValue("total");
                        jsonObj.put("delay_avg", df.format((sumDelay / 1000.0) / total));
                        jsonObj.put("sumDelay", sumDelay);
                        jsonObj.put("total", total);
                        cacheState.update(JSONObject.parseObject(jsonObj.toJSONString()));
                        jsonObj.remove("sumDelay");
                        jsonObj.remove("total");
                        jsonObj.put("is_delete", 0);
                        return jsonObj.toJSONString();
                    }
                });
//        SinkFunction<String> srSink = JobUtils.getStarrocksSink("upload_log_checkhour_temp");
        SinkFunction<String> srSink = StarRocksSink.sink(
                StarRocksSinkOptions.builder()
                        .withProperty("jdbc-url", "jdbc:mysql://10.17.41.138:9030?nuc_db")
                        .withProperty("load-url", "10.17.41.138:8030")
                        .withProperty("database-name", "nuc_db")
                        .withProperty("username", "huquan")
                        .withProperty("password", "oNa46nj0o65b@kvK")
                        .withProperty("table-name", "upload_log_checkhour_temp")
                        .withProperty("sink.properties.format", "json")
                        .withProperty("sink.properties.strip_outer_array", "true")
                        // TODO 删除测试代码
                        .withProperty("sink.buffer-flush.interval-ms", "1000")
                        // 设置并行度，多并行度情况下需要考虑如何保证数据有序性
                        .withProperty("sink.parallelism", "1")
                        .build()
        );
        resultS.addSink(srSink);
        env.execute("huquan_TestWindowStat");

    }

    private static void _toReport(JSONObject json, Collector<TestReportData> collector) {
        JSONArray data = json.getJSONArray("data");
        if (data != null && data.size() > 0) {
            for (int i = 0; i < data.size(); i++) {
                JSONObject result = new JSONObject();
                result.put("numberReport", json.getLong("numberReport"));
                result.put("interfaceRecTime", json.get("interfaceRecTime"));
                result.put("userName", json.get("userName"));
                result.put("clientId", json.get("clientId"));
                result.put("dropDataNum", json.get("dropDataNum"));
                result.put("timestamp", json.get("pt"));

                JSONObject jsonObj = data.getJSONObject(i);
                result.put("id", jsonObj.get("id"));
                result.put("submitId", jsonObj.get("submitId"));
                result.put("areaId", jsonObj.get("areaId"));
                result.put("recordId", jsonObj.get("recordId"));
                result.put("personIdCard", jsonObj.get("personIdCard"));
                result.put("personIdCardType", jsonObj.get("personIdCardType"));
                result.put("personName", jsonObj.get("personName"));
                result.put("personPhone", jsonObj.get("personPhone"));
                result.put("collectLocationName", jsonObj.get("collectLocationName"));
                result.put("collectLocationProvince", jsonObj.get("collectLocationProvince"));
                result.put("collectLocationCity", jsonObj.get("collectLocationCity"));
                result.put("collectLocationDistrict", jsonObj.get("collectLocationDistrict"));
                result.put("collectLocationStreet", jsonObj.get("collectLocationStreet"));
                result.put("collectTypeId", jsonObj.get("collectTypeId"));
                result.put("collectPartId", jsonObj.get("collectPartId"));
                result.put("tubeCode", jsonObj.get("tubeCode"));
                result.put("collectUser", jsonObj.get("collectUser"));
                result.put("collectTime", jsonObj.get("collectTime"));
                result.put("packTime", jsonObj.get("packTime"));
                result.put("deliveryCode", jsonObj.get("deliveryCode"));
                result.put("collectOrgName", jsonObj.get("collectOrgName"));
                result.put("checkOrgName", jsonObj.get("checkOrgName"));
                result.put("receiveTime", jsonObj.get("receiveTime"));
                result.put("receiveUser", jsonObj.get("receiveUser"));
                result.put("personId", jsonObj.get("personId"));
                result.put("checkTime", jsonObj.get("checkTime"));
                result.put("checkUser", jsonObj.get("checkUser"));
                result.put("checkResult", jsonObj.get("checkResult"));
                result.put("iggResult", jsonObj.get("iggResult"));
                result.put("igmResult", jsonObj.get("igmResult"));
                result.put("remark", jsonObj.get("remark"));
                result.put("addTime", jsonObj.get("addtime"));
                result.put("collectLimitnum", jsonObj.get("collectLimitnum"));
                result.put("collectCount", jsonObj.get("collectCount"));
//                System.out.println(result);
                collector.collect(result.toJavaObject(TestReportData.class));
            }
        }
    }
}
