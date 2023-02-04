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
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.text.SimpleDateFormat;
import java.util.Date;

// 采样点维度统计任务
public class CollectDistrictStat {
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

        Table unionTable = _unionStat(tEnv);
        tEnv.createTemporaryView("union_table", unionTable);

        // 注册check_org表
        tEnv.executeSql("CREATE TEMPORARY TABLE check_org ( " +
                "  id BIGINT, " +
                "  org_name string, " +
                "  PRIMARY KEY(id) NOT ENFORCED) " +
                "WITH ( " +
                "  'lookup.cache.max-rows' = '1000', " +
                "  'lookup.cache.ttl' = '24 hour', " +
                "  'connector' = 'jdbc', " +
                "  'driver' = 'com.mysql.cj.jdbc.Driver', " +
                "  'url' = 'jdbc:mysql://10.17.41.138:9030/nuc_db', " +
                "  'username' = 'huquan', " +
                "  'password' = 'oNa46nj0o65b@kvK', " +
                "  'table-name' = 'check_org' " +
                ")");

        // 关联org_id
        Table joinTable = tEnv.sqlQuery("select " +
                "t1.union_id union_id, " +
                "t1.area_id area_id, " +
                "t2.id org_code, " +
                "t1.org_name org_name, " +
                "t1.collect_location_name collect_location_name, " +
                "t1.source source, " +
                "t1.upload_number upload_number, " +
                "t1.success_number success_number, " +
                "t1.tube_number tube_number, " +
                "t1.upload_time upload_time, " +
                "t1.create_by create_by, " +
                "t1.collect_location_district collect_location_district " +
                "from union_table as t1 " +
                "left join check_org FOR SYSTEM_TIME AS OF t1.pt as t2 " +
                "on t1.org_name=t2.org_name");
        joinTable.printSchema();
        DataStream<Row> joinStream = tEnv.toAppendStream(joinTable, Row.class);

        SingleOutputStreamOperator<String> resultStream = joinStream.keyBy(row -> row.getField(0))
                .map(new RichMapFunction<Row, String>() {
                    private ValueState<JSONObject> cacheState;
                    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // TODO 修改其他任务缓存名
                        ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("CollectDistrictStat", JSONObject.class);
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(24))
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
                            cache.put("create_time", sdf.format(new Date()));
                        }
                        result.put("id", cache.getString("id"));
                        result.put("area_id", row.getField(1));
                        result.put("org_code", row.getField(2));
                        result.put("org_name", row.getField(3));
                        result.put("collect_location_name", row.getField(4));
                        result.put("source", row.getField(5));
                        result.put("upload_number", (long) row.getField(6) + cache.getLongValue("upload_number"));
                        result.put("success_number", (long) row.getField(7) + cache.getLongValue("success_number"));
//                        result.put("fail_number", null);
                        result.put("tube_number", (long) row.getField(8) + cache.getLongValue("tube_number"));
                        result.put("upload_time", row.getField(9));
                        result.put("create_by", row.getField(10));
                        result.put("create_time", cache.getString("create_time"));
                        result.put("is_delete", 0);
                        result.put("collect_location_district", row.getField(11));
                        cacheState.update(result);
                        return result.toJSONString();
                    }
                });

        SinkFunction<String> srSink = StarRocksSink.sink(
                StarRocksSinkOptions.builder()
                        .withProperty("jdbc-url", "jdbc:mysql://10.17.41.138:9030?nuc_db")
                        .withProperty("load-url", "10.17.41.138:8030")
                        .withProperty("database-name", "nuc_db")
                        .withProperty("username", "huquan")
                        .withProperty("password", "oNa46nj0o65b@kvK")
                        .withProperty("table-name", "upload_log_org")
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
        } catch (Exception ignored) {
        }
    }


    private static Table _unionStat(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery("select " +
                "concat_ws('_',cast(areaId as string),collectOrgName,collectLocationDistrict,collectLocationName,'1') union_id, " +
                "areaId area_id, " +
                "collectOrgName org_name, " +
                "collectLocationName collect_location_name, " +
                "1 source, " +
                "count(1) upload_number, " +
                "count(distinct concat(collectTime,personIdCard)) success_number, " +
                "count(distinct tubeCode) tube_number, " +
                "max(interfaceRecTime) upload_time, " +
                "max(userName) create_by, " +
                "PROCTIME() as pt, " +
                "collectLocationDistrict collect_location_district " +
                "from collect_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' SECOND), " +
                "areaId,collectOrgName,collectLocationDistrict,collectLocationName,'1' " +
                "union all " +
                // transport
                "select " +
                "concat_ws('_',cast(areaId as string),transportOrgName,'',collectAddress,'2') union_id, " +
                "areaId area_id, " +
                "transportOrgName org_name, " +
                "collectAddress collect_location_name, " +
                "2 source, " +
                "count(1) upload_number, " +
                "0 success_number, " +
                "sum(tubeNum) tube_number, " +
                "max(interfaceRecTime) upload_time, " +
                "max(userName) create_by, " +
                "PROCTIME() as pt, " +
                "'' collect_location_district " +
                "from transport_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' SECOND), " +
                "areaId,transportOrgName,'',collectAddress,'2' " +
                "union all " +
                // receive
                "select " +
                "concat_ws('_',cast(areaId as string),receiveOrgName,'','','3') union_id, " +
                "areaId area_id, " +
                "receiveOrgName org_name, " +
                "'' collect_location_name, " +
                "3 source, " +
                "count(1) upload_number, " +
                "0 success_number, " +
                "sum(tubeNum) tube_number, " +
                "max(interfaceRecTime) upload_time, " +
                "max(userName) create_by, " +
                "PROCTIME() as pt, " +
                "'' collect_location_district " +
                "from receive_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' SECOND), " +
                "areaId,receiveOrgName,'','','3' " +
                "union all " +
                // report
                "select " +
                "concat_ws('_',cast(areaId as string),checkOrgName,collectLocationDistrict,collectLocationName,'4') union_id, " +
                "areaId area_id, " +
                "checkOrgName org_name, " +
                "collectLocationName collect_location_name, " +
                "4 source, " +
                "count(1) upload_number, " +
                "count(distinct concat(collectTime,personIdCard)) success_number, " +
                "count(distinct tubeCode) tube_number, " +
                "max(interfaceRecTime) upload_time, " +
                "max(userName) create_by, " +
                "PROCTIME() as pt, " +
                "collectLocationDistrict collect_location_district " +
                "from report_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' SECOND), " +
                "areaId,checkOrgName,collectLocationDistrict,collectLocationName,'4' " +
                "");
    }
}
