package com.sxhs.realtime.window;

import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.sxhs.realtime.bean.ReportDataId;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.flink.table.api.Expressions.$;

// 采检时间差统计任务
public class CollectAndReportTimeDifStat {
    private static final Logger logger = LoggerFactory.getLogger(CollectAndReportTimeDifStat.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));

        //env.setStateBackend(new HashMapStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/211126/ck");
        //System.setProperty("HADOOP_USER_NAME", "atguigu");
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> sourceStream = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<ReportDataId> reportStream = sourceStream.flatMap(new FlatMapFunction<String, ReportDataId>() {
            @Override
            public void flatMap(String value, Collector<ReportDataId> collector) throws Exception {
                try {
                    JSONObject jsonObj = JSONObject.parseObject(value);
                    String type = jsonObj.getString("type");
                    if (type != null) {
                        if (type.equals("REPORT_DATA") || type.equals("XA_REPORT_DATA")) {
                            collector.collect(JSONObject.parseObject(value, ReportDataId.class));
                        }
                    } else {
                        logger.error("null type error: {}", value);
                    }
                } catch (Exception e) {
                    logger.error("json parse error: {}", value);
                }
            }
        });
        Table reportTable = tEnv.fromDataStream(reportStream, $("areaId"), $("addTime"), $("collectTime"), $("checkTime"),
                $("personId"), $("pt").proctime());
        tEnv.createTemporaryView("report", reportTable);
        String statSql = "select " +
                "concat_ws('_',cast(areaId as string),CAST(CURRENT_DATE as string),cast(TIMESTAMPDIFF(HOUR,TO_TIMESTAMP(collectTime),TO_TIMESTAMP(checkTime)) as string)) union_id, " +
                "areaId area_id, " +
                "TIMESTAMPDIFF(HOUR,TO_TIMESTAMP(collectTime),TO_TIMESTAMP(checkTime)) unit_hour, " +
                "count(personId) check_persons " +
//                "LOCALTIMESTAMP create_time, " +
//                "0 is_delete " +
                "from report " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' SECOND), " +
                "TIMESTAMPDIFF(HOUR,TO_TIMESTAMP(collectTime),TO_TIMESTAMP(checkTime)), " +
                "areaId";
        Table statTable = tEnv.sqlQuery(statSql);
//        statTable.execute().print();
        DataStream<Row> statStream = tEnv.toAppendStream(statTable, Row.class);
        SingleOutputStreamOperator<String> resultStream = statStream.keyBy((KeySelector<Row, Object>) row -> row.getField(0))
                .map(new RichMapFunction<Row, String>() {
                    private ValueState<JSONObject> cacheState;
                    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("CollectAndReportTimeDifStat", JSONObject.class);
//                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(24))
                        // TODO 删除测试代码
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(10))
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
                        jsonObj.put("check_persons", row.getField(3));

                        if (cache == null) {
                            // TODO 生成id
                            jsonObj.put("id", "123456");
                            jsonObj.put("create_time", sdf.format(new Date()));
                        } else {
                            jsonObj.put("id", cache.getLong("id"));
                            jsonObj.put("create_time", cache.getString("create_time"));
                            jsonObj.put("check_persons", jsonObj.getInteger("check_persons") + cache.getInteger("check_persons"));
                        }
                        cacheState.update(jsonObj);
                        jsonObj.put("is_delete", 0);
                        return jsonObj.toJSONString();
                    }
                });

        SinkFunction<String> srSink = StarRocksSink.sink(
                StarRocksSinkOptions.builder()
                        .withProperty("jdbc-url", "jdbc:mysql://10.17.41.138:9030?test_huq")
                        .withProperty("load-url", "10.17.41.138:8030")
                        .withProperty("database-name", "test_huq")
                        .withProperty("username", "root")
                        .withProperty("password", "SxHs@20230113")
                        .withProperty("table-name", "upload_log_checkhour")
                        .withProperty("sink.properties.format", "json")
                        .withProperty("sink.properties.strip_outer_array", "true")
                        // TODO 删除测试代码
                        .withProperty("sink.buffer-flush.interval-ms", "1000")
                        // 设置并行度，多并行度情况下需要考虑如何保证数据有序性
                        .withProperty("sink.parallelism", "1")
                        .build()
        );
        // TODO 删除测试代码
        resultStream.print();
        resultStream.addSink(srSink);
        env.execute("CollectAndReportTimeDifStat");

    }
}
